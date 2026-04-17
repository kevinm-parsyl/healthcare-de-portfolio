from datetime import datetime
import json
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from include.gx.gx_context import (
    build_raw_checkpoint,
    build_staging_checkpoint,
    run_checkpoint_or_raise,
)


PIPELINE_CONFIG = {
    'organization': {
        'stage': '@HEALTHCARE_DEV.RAW.FHIR_ORGANIZATION_STAGE',
        'table': 'HEALTHCARE_DEV.RAW.ORGANIZATION'
    },
    'practitioner': {
        'stage': '@HEALTHCARE_DEV.RAW.FHIR_PRACTITIONER_STAGE',
        'table': 'HEALTHCARE_DEV.RAW.PRACTITIONER'
    },
    'patient': {
        'stage': '@HEALTHCARE_DEV.RAW.FHIR_PATIENT_STAGE',
        'table': 'HEALTHCARE_DEV.RAW.BUNDLE'
    }
}

MANIFEST_TABLE = 'HEALTHCARE_DEV.RAW.LOAD_MANIFEST'
FILE_FORMAT    = 'HEALTHCARE_DEV.RAW.FHIR_JSON'

# @dag decorator turns this function into a DAG object.
# Everything inside it is part of the DAG definition.
@dag(
    dag_id="fhir_ingestion",
    schedule=None,                    # manual trigger only
    start_date=datetime(2024, 1, 1),  # required by Airflow, not meaningful at schedule=None
    catchup=False,                    # don't backfill missed runs
    tags=["healthcare", "fhir"],
    params={
        "measure_period_start": "2023-01-01",
        "measure_period_end": "2023-12-31",
    },
)


def _update_manifest_status(hook: SnowflakeHook, run_id: str,
                            result: dict, error: Exception = None) -> None:
    """
    Update manifest record after COPY INTO completes or fails.
    Private helper — not an Airflow task.
    """

    if result['rows_loaded'] == 0 and result['rows_rejected'] == 0:
        status = 'SKIPPED'
    elif result['rows_rejected'] == 0:
        status = 'LOADED'
    elif result['rows_loaded'] > 0:
        status = 'PARTIAL'
    else:
        status = 'FAILED'

    error_message = None
    if error:
        error_message = json.dumps({
            'error_type':    type(error).__name__,
            'error_message': str(error),
            'timestamp':     datetime.utcnow().isoformat()
        })

    hook.run(
        f"""
        UPDATE {MANIFEST_TABLE}
        SET
            load_status   = %(status)s,
            rows_loaded   = %(rows_loaded)s,
            rows_rejected = %(rows_rejected)s,
            error_message = %(error_message)s
        WHERE load_id = %(load_id)s
        """,
        parameters={
            'status':        status,
            'rows_loaded':   result['rows_loaded'],
            'rows_rejected': result['rows_rejected'],
            'error_message': error_message,
            'load_id':       run_id
        }
    )



def _copy_into_raw(hook: SnowflakeHook, table_name: str,
                stage_name: str, run_id: str) -> dict:
    """
    Execute COPY INTO from stage to raw table.
    Private helper — called by each copy task, not an Airflow task itself.
    """
    rows = hook.get_records(
        f"""
        COPY INTO {table_name} (
            raw_bundle,
            load_id,
            source_filename,
            file_row_number
        )
        FROM (
            SELECT
                $1,
                '{run_id}',
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER
            FROM {stage_name}
        )
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
        ON_ERROR    = 'CONTINUE'
        PURGE       = FALSE
        """
    )

    if not rows or len(rows[0]) < 4:
        return {'rows_loaded': 0, 'rows_rejected': 0}

    return {
        'rows_loaded':   sum(r[3] for r in rows),
        'rows_rejected': sum(r[5] for r in rows)
    }




def fhir_ingestion_dag():

    # @task decorator turns a Python function into an Airflow task.
    # Return values are automatically passed to downstream tasks (XCom).

    @task
    def get_stage_metadata(**context) -> dict:
        """
        LIST all three Snowflake stages and return metadata per stage.
        Refreshes directory metadata before listing.
        Returns dict of stage_name -> {file_count, total_bytes}.
        """
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        results = {}

        for resource_type, config in PIPELINE_CONFIG.items():
            stage = config['stage']
            stage_bare = stage.replace('@', '')

            hook.run(f"ALTER STAGE {stage_bare} REFRESH")
            rows = hook.get_records(f"LIST {stage}")

            results[resource_type] = {
                'stage':       stage,
                'file_count':  len(rows),
                'total_bytes': sum(row[1] for row in rows)
            }

        return results


    @task
    def insert_manifest_record(stage_metadata: dict, **context) -> None:
        """
        Insert PENDING manifest records for all three stages.
        Uses Airflow run_id as load_id — ties manifest to DAG run.
        """
        run_id = context["run_id"]
        hook   = SnowflakeHook(snowflake_conn_id="snowflake_default")

        for resource_type, meta in stage_metadata.items():
            hook.run(
                f"""
                INSERT INTO {MANIFEST_TABLE}
                    (load_id, stage_name, file_count, total_bytes, load_status)
                VALUES
                    (%(load_id)s, %(stage_name)s, %(file_count)s,
                    %(total_bytes)s, 'PENDING')
                """,
                parameters={
                    'load_id':     run_id,
                    'stage_name':  meta['stage'],
                    'file_count':  meta['file_count'],
                    'total_bytes': meta['total_bytes']
                }
            )


    @task
    def copy_into_raw_orgs(**context) -> dict:
        run_id = context["run_id"]
        hook   = SnowflakeHook(snowflake_conn_id="snowflake_default")
        config = PIPELINE_CONFIG['organization']
        result = _copy_into_raw(hook, config['table'], config['stage'], run_id)
        _update_manifest_status(hook, run_id, result)
        return result


    @task
    def copy_into_raw_practitioners(**context) -> dict:
        run_id = context["run_id"]
        hook   = SnowflakeHook(snowflake_conn_id="snowflake_default")
        config = PIPELINE_CONFIG['practitioner']
        result = _copy_into_raw(hook, config['table'], config['stage'], run_id)
        _update_manifest_status(hook, run_id, result)
        return result


    @task
    def copy_into_raw_patients(**context) -> dict:
        run_id = context["run_id"]
        hook   = SnowflakeHook(snowflake_conn_id="snowflake_default")
        config = PIPELINE_CONFIG['patient']
        result = _copy_into_raw(hook, config['table'], config['stage'], run_id)
        _update_manifest_status(hook, run_id, result)
        return result


    @task
    def fix_superseded(**context):
        run_id = context["run_id"]
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        # RAW.BUNDLE — supersede by patient_id
        hook.run(
            """
            UPDATE RAW.BUNDLE
            SET is_superseded = TRUE
            WHERE raw_bundle_id IN (
                -- Find surrogates belonging to prior loads
                -- for patients that appear in the current load
                SELECT b_old.raw_bundle_id
                FROM RAW.BUNDLE b_old,
                    LATERAL FLATTEN(input => b_old.raw_bundle:entry) f_old
                WHERE f_old.value:resource:resourceType::STRING = 'Patient'
                AND f_old.value:resource:id::STRING IN (
                    -- Patient FHIR IDs in current load
                    SELECT f_new.value:resource:id::STRING
                    FROM RAW.BUNDLE b_new,
                        LATERAL FLATTEN(input => b_new.raw_bundle:entry) f_new
                    WHERE f_new.value:resource:resourceType::STRING = 'Patient'
                    AND b_new.load_id = %(run_id)s
                )
                AND b_old.load_id != %(run_id)s
            )
            """,
            parameters={"run_id": run_id}
        )

        # RAW.ORGANIZATION — supersede by organization_id
        hook.run(
            """
            UPDATE RAW.ORGANIZATION
            SET is_superseded = TRUE
            WHERE raw_organization_id IN (
                -- Find surrogates belonging to prior loads
                -- for organizations that appear in the current load
                SELECT o_old.raw_organization_id
                FROM RAW.ORGANIZATION o_old,
                    LATERAL FLATTEN(input => o_old.raw_bundle:entry) f_old
                WHERE f_old.value:resource:resourceType::STRING = 'Organization'
                AND f_old.value:resource:id::STRING IN (
                    -- Organization FHIR IDs in current load
                    SELECT f_new.value:resource:id::STRING
                    FROM RAW.ORGANIZATION o_new,
                        LATERAL FLATTEN(input => o_new.raw_bundle:entry) f_new
                    WHERE f_new.value:resource:resourceType::STRING = 'Organization'
                    AND o_new.load_id = %(run_id)s
                )
                AND o_old.load_id != %(run_id)s
            )
            """,
            parameters={"run_id": run_id}
        )

        # RAW.PRACTITIONER — supersede by practitioner_id
        hook.run(
            """
            UPDATE RAW.PRACTITIONER
            SET is_superseded = TRUE
            WHERE raw_practitioner_id IN (
                -- Find surrogates belonging to prior loads
                -- for practitioner that appear in the current load
                SELECT p_old.raw_practitioner_id
                FROM RAW.PRACTITIONER p_old,
                    LATERAL FLATTEN(input => p_old.raw_bundle:entry) f_old
                WHERE f_old.value:resource:resourceType::STRING = 'Practitioner'
                AND f_old.value:resource:id::STRING IN (
                    -- practitioner FHIR IDs in current load
                    SELECT f_new.value:resource:id::STRING
                    FROM RAW.PRACTITIONER p_new,
                        LATERAL FLATTEN(input => p_new.raw_bundle:entry) f_new
                    WHERE f_new.value:resource:resourceType::STRING = 'Practitioner'
                    AND p_new.load_id = %(run_id)s
                )
                AND p_old.load_id != %(run_id)s
            )
            """,
            parameters={"run_id": run_id}
        )


    @task
    def validate_raw_ge():
        checkpoint = build_raw_checkpoint()
        return run_checkpoint_or_raise(checkpoint, "raw_bundle_checkpoint")

    @task
    def validate_staging_ge():
        checkpoint = build_staging_checkpoint()
        return run_checkpoint_or_raise(checkpoint, "staging_clinical_checkpoint")

    # BashOperator is not a @task — it's instantiated directly.
    # bash_command will point at your dbt project once we configure the mount.
    dbt_staging_run = BashOperator(
        task_id="dbt_staging_run",
        bash_command="echo 'dbt staging placeholder'",
    )

    dbt_mart_run = BashOperator(
        task_id="dbt_mart_run",
        bash_command="echo 'dbt mart placeholder'",
    )

    @task
    def write_pipeline_run_log():
        pass

    # Instantiate all @task functions so they become task objects
    stage_metadata = get_stage_metadata()
    manifest_record = insert_manifest_record(stage_metadata)
    ge_raw = validate_raw_ge()
    ge_staging = validate_staging_ge()
    load_orgs = copy_into_raw_orgs()
    load_practitioners = copy_into_raw_practitioners()
    load_patients = copy_into_raw_patients()
    superseded = fix_superseded()
    run_log = write_pipeline_run_log()

    # Dependency chain — parallel load where independent
    (
        stage_metadata
        >> manifest_record
        >> [load_orgs, load_practitioners]
        >> load_patients
        >> ge_raw
        >> superseded
        >> dbt_staging_run
        >> ge_staging
        >> dbt_mart_run
        >> run_log
    )

# This line instantiates the DAG — required for Airflow to discover it
fhir_ingestion_dag()