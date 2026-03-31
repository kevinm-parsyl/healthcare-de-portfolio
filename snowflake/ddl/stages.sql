-- Patient bundle stage
CREATE OR REPLACE STAGE HEALTHCARE_DEV.RAW.FHIR_PATIENT_STAGE
    ENCRYPTION                  = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT                 = HEALTHCARE_DEV.RAW.FHIR_JSON
    COMMENT                     = 'Internal stage for FHIR R4 patient bundle JSON files';

-- Organization/facility stage
CREATE OR REPLACE STAGE HEALTHCARE_DEV.RAW.FHIR_ORGANIZATION_STAGE
    ENCRYPTION                  = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT                 = HEALTHCARE_DEV.RAW.FHIR_JSON
    COMMENT                     = 'Internal stage for FHIR R4 organization/facility JSON files';

-- Practitioner stage
CREATE OR REPLACE STAGE HEALTHCARE_DEV.RAW.FHIR_PRACTITIONER_STAGE
    ENCRYPTION                  = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT                 = HEALTHCARE_DEV.RAW.FHIR_JSON
    COMMENT                     = 'Internal stage for FHIR R4 practitioner JSON files';