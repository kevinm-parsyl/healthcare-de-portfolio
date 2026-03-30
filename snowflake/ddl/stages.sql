-- File Format for FHIR R4 JSON bundles
CREATE OR REPLACE FILE FORMAT HEALTHCARE_DEV.RAW.FHIR_JSON
    TYPE                     = JSON
    STRIP_OUTER_ARRAY        = FALSE
    STRIP_NULL_VALUES        = FALSE
    REPLACE_INVALID_CHARACTERS = FALSE
    IGNORE_UTF8_ERRORS       = FALSE
    COMPRESSION              = AUTO
    COMMENT                  = 'File format for FHIR R4 JSON bundles from Synthea and compatible sources';

-- Internal stage for FHIR bundle files
CREATE OR REPLACE STAGE HEALTHCARE_DEV.RAW.FHIR_STAGE
    ENCRYPTION               = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT              = HEALTHCARE_DEV.RAW.FHIR_JSON
    COMMENT                  = 'Internal stage for FHIR R4 bundle JSON files. Snowflake-managed AES-256 encryption.';