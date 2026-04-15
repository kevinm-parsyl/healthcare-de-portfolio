import great_expectations as gx
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_snowflake_connection_string(schema: str) -> str:
    """
    Retrieves Snowflake credentials from Airflow's connection store
    and builds a SQLAlchemy connection string for GE.
    Schema is passed explicitly to avoid fragile string manipulation.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_connection("snowflake_default")
    return (
        f"snowflake://{conn.login}:{conn.password}@"
        f"{conn.extra_dejson.get('account')}/"
        f"{conn.extra_dejson.get('database')}/"
        f"{schema}"
        f"?warehouse={conn.extra_dejson.get('warehouse')}"
        f"&role={conn.extra_dejson.get('role')}"
    )


def build_raw_checkpoint(
    min_rows: int = 1000,
    max_rows: int = 40000,
) -> gx.checkpoint.Checkpoint:
    """
    Checkpoint 1: Structural/operational validation against RAW.BUNDLE
    after COPY INTO, before dbt runs.

    NOTE: In a production environment sourcing from a conformant FHIR
    server (e.g. Epic FHIR API), structural FHIR validation would be
    unnecessary here — the server guarantees R4 conformance. These checks
    are scoped to operational pipeline behavior: correct volumes, no
    nulls, no duplicates within batch.
    """
    context = gx.get_context(mode="ephemeral")

    datasource = context.sources.add_snowflake(
        name="snowflake_raw",
        connection_string=get_snowflake_connection_string("RAW"),
    )

    data_asset = datasource.add_table_asset(
        name="raw_bundle",
        table_name="BUNDLE",
        schema_name="RAW",
    )

    suite = context.add_expectation_suite("raw_bundle_quality")

    # Operational: batch size within expected range
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=min_rows,
            max_value=max_rows,
        )
    )

    # Structural: no null raw_bundle values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="RAW_BUNDLE",
        )
    )

    # Structural: no duplicate bundle_ids within batch
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="BUNDLE_ID",
        )
    )

    # Operational: entry array non-empty — queries inside VARIANT
    suite.add_expectation(
        gx.expectations.UnexpectedRowsExpectation(
            unexpected_rows_query="""
                SELECT bundle_id
                FROM RAW.BUNDLE
                WHERE raw_bundle:entry IS NULL
                OR ARRAY_SIZE(raw_bundle:entry) = 0
            """,
        )
    )

    checkpoint = context.add_checkpoint(
        name="raw_bundle_checkpoint",
        validations=[
            {
                "batch_request": data_asset.build_batch_request(),
                "expectation_suite_name": "raw_bundle_quality",
            }
        ],
    )

    return checkpoint


def build_staging_checkpoint() -> gx.checkpoint.Checkpoint:
    """
    Checkpoint 2: Clinical quality validation against DEV_STAGING
    after dbt staging run, before dbt mart run.
    Validates decomposed columns — clinical distributions and
    referential sanity checks that dbt tests don't cover.
    """
    context = gx.get_context(mode="ephemeral")

    datasource = context.sources.add_snowflake(
        name="snowflake_staging",
        connection_string=get_snowflake_connection_string("DEV_STAGING"),
    )

    data_asset = datasource.add_table_asset(
        name="stg_observation",
        table_name="STG_OBSERVATION",
        schema_name="DEV_STAGING",
    )

    suite = context.add_expectation_suite("staging_clinical_quality")

    # Clinical: HbA1c values within physiologically plausible range
    # LOINC 4548-4 = HbA1c (%). Values outside 3.0-20.0 = instrument error.
    suite.add_expectation(
        gx.expectations.UnexpectedRowsExpectation(
            unexpected_rows_query="""
                SELECT observation_id, observation_value
                FROM DEV_STAGING.STG_OBSERVATION
                WHERE observation_code = '4548-4'
                AND observation_value::FLOAT NOT BETWEEN 3.0 AND 20.0
            """,
        )
    )

    # Operational: no null patient references
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="PATIENT_ID",
        )
    )

    # Operational: row count sanity check
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=10000,
            max_value=500000,
        )
    )

    checkpoint = context.add_checkpoint(
        name="staging_clinical_checkpoint",
        validations=[
            {
                "batch_request": data_asset.build_batch_request(),
                "expectation_suite_name": "staging_clinical_quality",
            }
        ],
    )

    return checkpoint


def run_checkpoint_or_raise(
    checkpoint: gx.checkpoint.Checkpoint,
    checkpoint_name: str,
) -> dict:
    """
    Runs a GE checkpoint and raises ValueError on failure.
    The raised exception is what halts downstream Airflow tasks —
    Airflow marks the task failed, downstream tasks enter
    upstream_failed state and do not execute.

    Returns results dict on success for consumption by
    write_pipeline_run_log task.
    """
    results = checkpoint.run()

    if not results.success:
        failed = [
            str(result)
            for result in results.run_results.values()
            if not result["success"]
        ]
        raise ValueError(
            f"GE checkpoint '{checkpoint_name}' failed. "
            f"Failed expectations: {failed}"
        )

    return {
        "checkpoint_name": checkpoint_name,
        "success": results.success,
        "evaluated_expectations": results.statistics.get(
            "evaluated_expectations", 0
        ),
        "successful_expectations": results.statistics.get(
            "successful_expectations", 0
        ),
    }