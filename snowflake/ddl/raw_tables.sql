CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DEV.RAW;

-- Batch-level load manifest
-- Tracks business metadata not captured by Snowflake or Airflow natively
-- load_id becomes Airflow run_id in Project 2
CREATE OR REPLACE TABLE HEALTHCARE_DEV.RAW.LOAD_MANIFEST (
    load_id         STRING          NOT NULL DEFAULT UUID_STRING(),
    stage_name      STRING          NOT NULL,
    file_count      INT,
    total_bytes     INT,
    rows_loaded     INT,
    rows_rejected   INT,
    load_status     STRING          NOT NULL DEFAULT 'PENDING',
    error_message   STRING,
    datetime_loaded TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    loaded_by       STRING          NOT NULL DEFAULT CURRENT_USER(),
    loaded_role     STRING          NOT NULL DEFAULT CURRENT_ROLE(),
    CONSTRAINT pk_load_manifest PRIMARY KEY (load_id)
);

-- Raw patient bundles
CREATE OR REPLACE TABLE HEALTHCARE_DEV.RAW.BUNDLE (
    raw_bundle_id   STRING          NOT NULL DEFAULT UUID_STRING(),
    raw_bundle      VARIANT         NOT NULL,
    load_id         STRING          NOT NULL,
    source_filename STRING          NOT NULL,
    file_row_number INT,
    source_system   STRING          NOT NULL DEFAULT 'synthea',
    is_superseded   BOOLEAN         NOT NULL DEFAULT FALSE,
    datetime_loaded TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    loaded_by       STRING          NOT NULL DEFAULT CURRENT_USER(),
    loaded_role     STRING          NOT NULL DEFAULT CURRENT_ROLE(),
    CONSTRAINT pk_bundle PRIMARY KEY (raw_bundle_id),
    CONSTRAINT fk_bundle_manifest FOREIGN KEY (load_id)
        REFERENCES HEALTHCARE_DEV.RAW.LOAD_MANIFEST(load_id)
)
CLUSTER BY (source_filename);

-- Raw organization/facility bundles
CREATE OR REPLACE TABLE HEALTHCARE_DEV.RAW.ORGANIZATION (
    raw_organization_id STRING      NOT NULL DEFAULT UUID_STRING(),
    raw_bundle          VARIANT     NOT NULL,
    load_id             STRING      NOT NULL,
    source_filename     STRING      NOT NULL,
    file_row_number     INT,
    source_system       STRING      NOT NULL DEFAULT 'synthea',
    is_superseded       BOOLEAN     NOT NULL DEFAULT FALSE,
    datetime_loaded     TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    loaded_by           STRING      NOT NULL DEFAULT CURRENT_USER(),
    loaded_role         STRING      NOT NULL DEFAULT CURRENT_ROLE(),
    CONSTRAINT pk_organization PRIMARY KEY (raw_organization_id),
    CONSTRAINT fk_organization_manifest FOREIGN KEY (load_id)
        REFERENCES HEALTHCARE_DEV.RAW.LOAD_MANIFEST(load_id)
)
CLUSTER BY (source_filename);

-- Raw practitioner bundles
CREATE OR REPLACE TABLE HEALTHCARE_DEV.RAW.PRACTITIONER (
    raw_practitioner_id STRING      NOT NULL DEFAULT UUID_STRING(),
    raw_bundle          VARIANT     NOT NULL,
    load_id             STRING      NOT NULL,
    source_filename     STRING      NOT NULL,
    file_row_number     INT,
    source_system       STRING      NOT NULL DEFAULT 'synthea',
    is_superseded       BOOLEAN     NOT NULL DEFAULT FALSE,
    datetime_loaded     TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    loaded_by           STRING      NOT NULL DEFAULT CURRENT_USER(),
    loaded_role         STRING      NOT NULL DEFAULT CURRENT_ROLE(),
    CONSTRAINT pk_practitioner PRIMARY KEY (raw_practitioner_id),
    CONSTRAINT fk_practitioner_manifest FOREIGN KEY (load_id)
        REFERENCES HEALTHCARE_DEV.RAW.LOAD_MANIFEST(load_id)
)
CLUSTER BY (source_filename);