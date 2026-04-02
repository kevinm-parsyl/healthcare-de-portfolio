{{
    config(
        materialized='view'
    )
}}
-- TODO Project 2: Add WHERE b.is_superseded = FALSE filter once
-- supersession logic is implemented in the ingestion pipeline.
-- Currently all bundles default to is_superseded=FALSE.

SELECT
    -- Patient identity
    f.value:resource:id::STRING                                         AS patient_id,
    mrn.value:value::STRING                                             AS medical_record_number,

    -- Demographics
    f.value:resource:name[0]:given[0]::STRING                          AS first_name,
    f.value:resource:name[0]:family::STRING                            AS last_name,
    f.value:resource:name[0]:prefix[0]::STRING                        AS name_prefix,
    TRY_CAST(f.value:resource:birthDate::STRING AS DATE)               AS birth_date,
    f.value:resource:gender::STRING                                    AS gender,

    -- Clinical extensions — extracted via WHERE-filtered FLATTENs, pivoted with MAX/CASE
    MAX(CASE WHEN birthsex_ext.value:url::STRING LIKE '%us-core-birthsex%'
        THEN birthsex_ext.value:valueCode::STRING END)                 AS birth_sex,

    MAX(CASE WHEN race_ext.value:url::STRING LIKE '%us-core-race%'
             AND race_inner.value:url::STRING = 'text'
        THEN race_inner.value:valueString::STRING END)                 AS race,

    MAX(CASE WHEN race_ext.value:url::STRING LIKE '%us-core-ethnicity%'
             AND eth_inner.value:url::STRING = 'text'
        THEN eth_inner.value:valueString::STRING END)                  AS ethnicity,

    -- Communication
    f.value:resource:communication[0]:language:coding[0]:code::STRING  AS primary_language_code,

    -- Marital status
    f.value:resource:maritalStatus:coding[0]:code::STRING              AS marital_status_code,
    f.value:resource:maritalStatus:coding[0]:display::STRING           AS marital_status_display,

    -- Deceased
    CASE
        WHEN f.value:resource:deceasedDateTime IS NOT NULL THEN TRUE
        WHEN f.value:resource:deceasedBoolean::BOOLEAN = TRUE THEN TRUE
        ELSE FALSE
    END                                                                 AS is_deceased,
    f.value:resource:deceasedDateTime::TIMESTAMP_TZ                    AS deceased_datetime,

    -- Birth
    f.value:resource:multipleBirthBoolean::BOOLEAN                     AS is_multiple_birth,

    -- Address
    f.value:resource:address[0]:line[0]::STRING                        AS address_line_1,
    f.value:resource:address[0]:city::STRING                           AS city,
    f.value:resource:address[0]:state::STRING                          AS state,
    f.value:resource:address[0]:postalCode::STRING                     AS postal_code,
    f.value:resource:address[0]:country::STRING                        AS country,

    -- Geolocation
    MAX(CASE WHEN geo_inner.value:url::STRING = 'latitude'
        THEN geo_inner.value:valueDecimal::FLOAT END)                  AS latitude,
    MAX(CASE WHEN geo_inner.value:url::STRING = 'longitude'
        THEN geo_inner.value:valueDecimal::FLOAT END)                  AS longitude,

    -- Contact
    MAX(CASE WHEN telecom.value:system::STRING = 'phone'
        THEN telecom.value:value::STRING END)                          AS phone,

    -- Metadata
    f.value:resource:meta:profile[0]::STRING                           AS fhir_profile,
    f.value:resource:identifier                                        AS identifiers_json,

    -- Lineage
    b.source_filename,
    b.datetime_loaded                                                   AS loaded_at

FROM {{ source('raw', 'bundle') }} b,
    LATERAL FLATTEN(input => b.raw_bundle:entry) f,
    LATERAL FLATTEN(input => f.value:resource:identifier) mrn,
    LATERAL FLATTEN(input => f.value:resource:extension) birthsex_ext,
    LATERAL FLATTEN(input => f.value:resource:extension) race_ext,
    LATERAL FLATTEN(input => race_ext.value:extension) race_inner,
    LATERAL FLATTEN(input => f.value:resource:extension) race_ext2,
    LATERAL FLATTEN(input => race_ext2.value:extension) eth_inner,
    LATERAL FLATTEN(input => f.value:resource:address[0]:extension) geo_ext,
    LATERAL FLATTEN(input => geo_ext.value:extension) geo_inner,
    LATERAL FLATTEN(input => f.value:resource:telecom) telecom

WHERE f.value:resource:resourceType::STRING = 'Patient'
    AND mrn.value:type:coding[0]:code::STRING = 'MR'
    AND geo_ext.value:url::STRING LIKE '%geolocation%'

GROUP BY
    f.value:resource:id::STRING,
    mrn.value:value::STRING,
    f.value:resource:name[0]:given[0]::STRING,
    f.value:resource:name[0]:family::STRING,
    f.value:resource:name[0]:prefix[0]::STRING,
    TRY_CAST(f.value:resource:birthDate::STRING AS DATE),
    f.value:resource:gender::STRING,
    f.value:resource:communication[0]:language:coding[0]:code::STRING,
    f.value:resource:maritalStatus:coding[0]:code::STRING,
    f.value:resource:maritalStatus:coding[0]:display::STRING,
    CASE
        WHEN f.value:resource:deceasedDateTime IS NOT NULL THEN TRUE
        WHEN f.value:resource:deceasedBoolean::BOOLEAN = TRUE THEN TRUE
        ELSE FALSE
    END,
    f.value:resource:deceasedDateTime::TIMESTAMP_TZ,
    f.value:resource:multipleBirthBoolean::BOOLEAN,
    f.value:resource:address[0]:line[0]::STRING,
    f.value:resource:address[0]:city::STRING,
    f.value:resource:address[0]:state::STRING,
    f.value:resource:address[0]:postalCode::STRING,
    f.value:resource:address[0]:country::STRING,
    f.value:resource:meta:profile[0]::STRING,
    f.value:resource:identifier,
    b.source_filename,
    b.datetime_loaded