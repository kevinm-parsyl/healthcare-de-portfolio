from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from include.gx.gx_context import (
    build_raw_checkpoint,
    build_staging_checkpoint,
    run_checkpoint_or_raise,
)

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
def fhir_ingestion_dag():

    # @task decorator turns a Python function into an Airflow task.
    # Return values are automatically passed to downstream tasks (XCom).
    @task
    def get_stage_metadata():
        pass

    @task
    def insert_manifest_record():
        pass

    @task
    def copy_into_raw_orgs():
        pass

    @task
    def copy_into_raw_practitioners():
        pass

    @task
    def copy_into_raw_patients():
        pass

    @task
    def fix_superseded():
        pass

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
    manifest_record = insert_manifest_record()
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