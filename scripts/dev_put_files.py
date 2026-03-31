"""
One time script to load synthea data to Snowflake stages.
Routes files to purpose-specific stages based on filename pattern.
"""

from dotenv import load_dotenv
import os
import snowflake.connector

load_dotenv()

SNOWFLAKE_ACCOUNT    = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER       = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD   = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE       = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE  = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE   = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA     = os.getenv('SNOWFLAKE_SCHEMA')

if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
            SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
            SNOWFLAKE_SCHEMA]):
    raise ValueError("One or more Snowflake credentials not set in .env")

FHIR_DIR             = '/Users/kevinmccreery/Documents/VSCode/healthcare-de-portfolio/output/fhir'
PATIENT_STAGE        = '@HEALTHCARE_DEV.RAW.FHIR_PATIENT_STAGE'
ORGANIZATION_STAGE   = '@HEALTHCARE_DEV.RAW.FHIR_ORGANIZATION_STAGE'
PRACTITIONER_STAGE   = '@HEALTHCARE_DEV.RAW.FHIR_PRACTITIONER_STAGE'

conn = None
cur  = None

try:
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    patient_count      = 0
    org_count          = 0
    practitioner_count = 0

    files = [f for f in os.listdir(FHIR_DIR) if f.endswith('.json')]
    print(f"Found {len(files)} files to upload")

    for filename in files:
        filepath = os.path.join(FHIR_DIR, filename)

        if 'hospitalInformation' in filename:
            stage = ORGANIZATION_STAGE
            org_count += 1
        elif 'practitionerInformation' in filename:
            stage = PRACTITIONER_STAGE
            practitioner_count += 1
        else:
            stage = PATIENT_STAGE
            patient_count += 1

        safe_filepath = filepath.replace("'", "\\'")
        cur.execute(f"PUT 'file://{safe_filepath}' {stage} OVERWRITE=FALSE PARALLEL=4")

    print(f"Uploaded to FHIR_PATIENT_STAGE:      {patient_count}")
    print(f"Uploaded to FHIR_ORGANIZATION_STAGE: {org_count}")
    print(f"Uploaded to FHIR_PRACTITIONER_STAGE: {practitioner_count}")

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()