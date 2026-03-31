CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DEV.RAW;

CREATE OR REPLACE TABLE HEALTHCARE_DEV.RAW.LOAD_MANIFEST (
    load_id             STRING          NOT NULL DEFAULT UUID_STRING(),
    source_filename     STRING          NOT NULL,
    source_file_size_bytes INT,
    datetime_received   TIMESTAMP_TZ    NOT NULL,
    checksum_md5        STRING          NOT NULL,
    load_status         STRING          NOT NULL DEFAULT 'PENDING',
    retry_count         INT             NOT NULL DEFAULT 0,
    error_message       STRING,
    loaded_by           STRING          NOT NULL DEFAULT CURRENT_USER(),
    loaded_role         STRING          NOT NULL DEFAULT CURRENT_ROLE()HEALTHCARE_DEV.RAW.FHIR_STAGE,
    datetime_loaded     TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_load_manifest PRIMARY KEY (load_id)
);

CREATE OR REPLACE TABLE HEALTHCARE_DEV.RAW.BUNDLE (
    raw_bundle_id       STRING          NOT NULL DEFAULT UUID_STRING(),
    raw_bundle          VARIANT         NOT NULL,
    load_id             STRING          NOT NULL,
    source_filename     STRING          NOT NULL,
    datetime_received   TIMESTAMP_TZ    NOT NULL,
    source_system       STRING          NOT NULL DEFAULT 'synthea',
    file_row_number     INT,
    is_superseded       BOOLEAN         NOT NULL DEFAULT FALSE,
    datetime_loaded     TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    loaded_by           STRING          NOT NULL DEFAULT CURRENT_USER(),
    loaded_role         STRING          NOT NULL DEFAULT CURRENT_ROLE(),
    CONSTRAINT pk_bundle PRIMARY KEY (raw_bundle_id),
    CONSTRAINT fk_bundle_manifest FOREIGN KEY (load_id) 
        REFERENCES HEALTHCARE_DEV.RAW.LOAD_MANIFEST(load_id)
)
CLUSTER BY (source_filename);


CREATE OR REPLACE TABLE HEALTHCARE_DEV.RAW.ORGANIZATION (
    raw_organization_id     STRING          NOT NULL DEFAULT UUID_STRING(),
    raw_bundle              VARIANT         NOT NULL,
    load_id                 STRING          NOT NULL,
    source_filename         STRING          NOT NULL,
    datetime_received       TIMESTAMP_TZ    NOT NULL,
    source_system           STRING          NOT NULL DEFAULT 'synthea',
    file_row_number         INT,
    is_superseded           BOOLEAN         NOT NULL DEFAULT FALSE,
    datetime_loaded         TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               STRING          NOT NULL DEFAULT CURRENT_USER(),
    loaded_role             STRING          NOT NULL DEFAULT CURRENT_ROLE(),
    CONSTRAINT pk_organization PRIMARY KEY (raw_organization_id),
    CONSTRAINT fk_organization_manifest FOREIGN KEY (load_id)
        REFERENCES HEALTHCARE_DEV.RAW.LOAD_MANIFEST(load_id)
)
CLUSTER BY (source_filename);

CREATE OR REPLACE TABLE HEALTHCARE_DEV.RAW.PRACTITIONER (
    raw_practitioner_id     STRING          NOT NULL DEFAULT UUID_STRING(),
    raw_bundle              VARIANT         NOT NULL,
    load_id                 STRING          NOT NULL,
    source_filename         STRING          NOT NULL,
    datetime_received       TIMESTAMP_TZ    NOT NULL,
    source_system           STRING          NOT NULL DEFAULT 'synthea',
    file_row_number         INT,
    is_superseded           BOOLEAN         NOT NULL DEFAULT FALSE,
    datetime_loaded         TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               STRING          NOT NULL DEFAULT CURRENT_USER(),
    loaded_role             STRING          NOT NULL DEFAULT CURRENT_ROLE(),
    CONSTRAINT pk_practitioner PRIMARY KEY (raw_practitioner_id),
    CONSTRAINT fk_practitioner_manifest FOREIGN KEY (load_id)
        REFERENCES HEALTHCARE_DEV.RAW.LOAD_MANIFEST(load_id)
)
CLUSTER BY (source_filename);
