{{
    config(
        materialized='view'
    )
}}

-- TODO Project 2: Add WHERE b.is_superseded = FALSE filter once
-- supersession logic is implemented in ingestion pipeline.
-- Currently filtering status='finished' as proxy for current record.
-- Staging model aligned with QI-Core STU 6 Encounter profile.

SELECT
    -- Encounter identity
    f.value:resource:id::STRING                                         AS encounter_id,
    SPLIT_PART(
        f.value:resource:subject:reference::STRING, 'urn:uuid:', 2
    )                                                                   AS patient_id,

    -- Encounter classification
    f.value:resource:status::STRING                                     AS status,
    f.value:resource:class:code::STRING                                 AS encounter_class_code,
    f.value:resource:class:system::STRING                               AS encounter_class_system,

    -- Encounter type (SNOMED)
    f.value:resource:type[0]:coding[0]:code::STRING                     AS type_code,
    f.value:resource:type[0]:coding[0]:display::STRING                  AS type_display,
    f.value:resource:type[0]:coding[0]:system::STRING                   AS type_system,
    f.value:resource:type[0]:coding                                     AS type_codings_json,

    -- Timing
    f.value:resource:period:start::TIMESTAMP_TZ                         AS encounter_start,
    f.value:resource:period:end::TIMESTAMP_TZ                           AS encounter_end,
    DATEDIFF(
        'minute',
        f.value:resource:period:start::TIMESTAMP_TZ,
        f.value:resource:period:end::TIMESTAMP_TZ
    )                                                                   AS encounter_duration_minutes,

    -- Reason for visit
    f.value:resource:reasonCode[0]:coding[0]:code::STRING               AS reason_code,
    f.value:resource:reasonCode[0]:coding[0]:display::STRING            AS reason_display,
    f.value:resource:reasonCode[0]:coding[0]:system::STRING             AS reason_system,

    -- Primary performer — extracted from participant array
    MAX(CASE
        WHEN pprf.value:type[0]:coding[0]:code::STRING = 'PPRF'
        THEN SPLIT_PART(
            pprf.value:individual:reference::STRING, '|', 2)
    END)                                                                AS practitioner_npi,

    MAX(CASE
        WHEN pprf.value:type[0]:coding[0]:code::STRING = 'PPRF'
        THEN pprf.value:individual:display::STRING
    END)                                                                AS practitioner_display,

    -- Service provider (organization)
    SPLIT_PART(
        f.value:resource:serviceProvider:reference::STRING, '|', 2
    )                                                                   AS service_provider_id,
    f.value:resource:serviceProvider:display::STRING                    AS service_provider_display,

    -- Location (first listed)
    f.value:resource:location[0]:location:display::STRING               AS location_display,

    -- Hospitalization (null for non-inpatient)
    f.value:resource:hospitalization:dischargeDisposition:coding[0]:code::STRING
                                                                        AS discharge_disposition_code,
    f.value:resource:hospitalization:dischargeDisposition:coding[0]:display::STRING
                                                                        AS discharge_disposition_display,

    -- Diagnosis linkage (QI-Core)
    f.value:resource:diagnosis[0]:condition:reference::STRING           AS primary_diagnosis_reference,
    f.value:resource:diagnosis[0]:use:coding[0]:code::STRING            AS primary_diagnosis_use_code,

    -- Convenience flags for CQM filtering
    CASE WHEN f.value:resource:class:code::STRING = 'EMER'
        THEN TRUE ELSE FALSE END                                        AS is_ed_encounter,
    CASE WHEN f.value:resource:class:code::STRING = 'IMP'
        THEN TRUE ELSE FALSE END                                        AS is_inpatient_encounter,

    -- Preserved arrays (QI-Core alignment)
    f.value:resource:type                                               AS types_json,
    f.value:resource:reasonCode                                         AS reason_codes_json,
    f.value:resource:participant                                        AS participants_json,
    f.value:resource:location                                           AS locations_json,
    f.value:resource:diagnosis                                          AS diagnoses_json,

    -- Lineage
    b.source_filename,
    b.datetime_loaded                                                   AS loaded_at

FROM {{ source('raw', 'bundle') }} b,
    LATERAL FLATTEN(input => b.raw_bundle:entry) f,
    LATERAL FLATTEN(input => f.value:resource:participant) pprf

WHERE f.value:resource:resourceType::STRING = 'Encounter'
    AND f.value:resource:status::STRING = 'finished'

GROUP BY
    f.value:resource:id::STRING,
    f.value:resource:subject:reference::STRING,
    f.value:resource:status::STRING,
    f.value:resource:class:code::STRING,
    f.value:resource:class:system::STRING,
    f.value:resource:type[0]:coding[0]:code::STRING,
    f.value:resource:type[0]:coding[0]:display::STRING,
    f.value:resource:type[0]:coding[0]:system::STRING,
    f.value:resource:type[0]:coding,
    f.value:resource:period:start::TIMESTAMP_TZ,
    f.value:resource:period:end::TIMESTAMP_TZ,
    DATEDIFF(
        'minute',
        f.value:resource:period:start::TIMESTAMP_TZ,
        f.value:resource:period:end::TIMESTAMP_TZ
    ),
    f.value:resource:reasonCode[0]:coding[0]:code::STRING,
    f.value:resource:reasonCode[0]:coding[0]:display::STRING,
    f.value:resource:reasonCode[0]:coding[0]:system::STRING,
    f.value:resource:serviceProvider:reference::STRING,
    f.value:resource:serviceProvider:display::STRING,
    f.value:resource:location[0]:location:display::STRING,
    f.value:resource:hospitalization:dischargeDisposition:coding[0]:code::STRING,
    f.value:resource:hospitalization:dischargeDisposition:coding[0]:display::STRING,
    f.value:resource:diagnosis[0]:condition:reference::STRING,
    f.value:resource:diagnosis[0]:use:coding[0]:code::STRING,
    CASE WHEN f.value:resource:class:code::STRING = 'EMER'
        THEN TRUE ELSE FALSE END,
    CASE WHEN f.value:resource:class:code::STRING = 'IMP'
        THEN TRUE ELSE FALSE END,
    f.value:resource:type,
    f.value:resource:reasonCode,
    f.value:resource:participant,
    f.value:resource:location,
    f.value:resource:diagnosis,
    b.source_filename,
    b.datetime_loaded