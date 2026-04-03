{{
    config(
        materialized='view'
    )
}}

-- TODO Project 2: Add WHERE b.is_superseded = FALSE filter once
-- supersession logic is implemented in ingestion pipeline.
-- Staging model aligned with QI-Core STU 6 Condition profile.
-- clinicalStatus and verificationStatus required for CMS122 denominator logic.

SELECT
    -- Condition identity
    f.value:resource:id::STRING                                         AS condition_id,
    SPLIT_PART(
        f.value:resource:subject:reference::STRING, 'urn:uuid:', 2
    )                                                                   AS patient_id,

    -- Clinical and verification status (required for CQM validity)
    f.value:resource:clinicalStatus:coding[0]:code::STRING              AS clinical_status,
    f.value:resource:verificationStatus:coding[0]:code::STRING          AS verification_status,

    -- Condition code (primary — SNOMED or ICD-10)
    f.value:resource:code:coding[0]:code::STRING                        AS condition_code,
    f.value:resource:code:coding[0]:display::STRING                     AS condition_display,
    f.value:resource:code:coding[0]:system::STRING                      AS condition_code_system,
    f.value:resource:code:text::STRING                                  AS condition_text,
    f.value:resource:code:coding                                        AS condition_codings_json,

    -- Onset timing (use onset[x] per QI-Core — condition start)
    -- COALESCE handles onsetDateTime vs onsetPeriod.start
    COALESCE(
        f.value:resource:onsetDateTime::TIMESTAMP_TZ,
        f.value:resource:onsetPeriod:start::TIMESTAMP_TZ
    )                                                                   AS onset_datetime,

    -- Abatement (when condition resolved)
    COALESCE(
        f.value:resource:abatementDateTime::TIMESTAMP_TZ,
        f.value:resource:abatementPeriod:start::TIMESTAMP_TZ
    )                                                                   AS abatement_datetime,

    -- Recorded date (when clinician documented it)
    f.value:resource:recordedDate::TIMESTAMP_TZ                        AS recorded_date,

    -- Category (encounter-diagnosis vs problem-list-item etc)
    f.value:resource:category[0]:coding[0]:code::STRING                 AS category_code,
    f.value:resource:category[0]:coding[0]:display::STRING              AS category_display,

    -- Encounter context
    SPLIT_PART(
        f.value:resource:encounter:reference::STRING, 'urn:uuid:', 2
    )                                                                   AS encounter_id,

    -- Convenience flag for CMS122 denominator
    CASE
        WHEN f.value:resource:verificationStatus:coding[0]:code::STRING = 'confirmed'
         AND f.value:resource:clinicalStatus:coding[0]:code::STRING
             IN ('active', 'recurrence', 'relapse')
        THEN TRUE
        ELSE FALSE
    END                                                                 AS is_active_confirmed,

    -- Preserved arrays (QI-Core alignment)
    f.value:resource:code:coding                                        AS codings_json,
    f.value:resource:category                                           AS categories_json,

    -- Lineage
    b.source_filename,
    b.datetime_loaded                                                   AS loaded_at

FROM {{ source('raw', 'bundle') }} b,
    LATERAL FLATTEN(input => b.raw_bundle:entry) f

WHERE f.value:resource:resourceType::STRING = 'Condition'