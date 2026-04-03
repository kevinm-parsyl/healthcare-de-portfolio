{{
    config(
        materialized='view'
    )
}}

-- TODO Project 2: Add WHERE b.is_superseded = FALSE filter once
-- supersession logic is implemented in ingestion pipeline.
-- Staging model aligned with QI-Core STU 6 Observation profile.
-- effectiveDateTime used per QI-Core for CMS122 (when test was performed).
-- valueQuantity pattern handles HbA1c numeric results (LOINC 4548-4 etc).

SELECT
    -- Observation identity
    f.value:resource:id::STRING                                         AS observation_id,
    SPLIT_PART(
        f.value:resource:subject:reference::STRING, 'urn:uuid:', 2
    )                                                                   AS patient_id,
    SPLIT_PART(
        f.value:resource:encounter:reference::STRING, 'urn:uuid:', 2
    )                                                                   AS encounter_id,

    -- Status
    f.value:resource:status::STRING                                     AS status,

    -- Observation code (LOINC for labs, SNOMED for clinical findings)
    f.value:resource:code:coding[0]:code::STRING                        AS observation_code,
    f.value:resource:code:coding[0]:display::STRING                     AS observation_display,
    f.value:resource:code:coding[0]:system::STRING                      AS observation_code_system,
    f.value:resource:code:text::STRING                                  AS observation_text,
    f.value:resource:code:coding                                        AS observation_codings_json,

    -- Category (laboratory, vital-signs, social-history etc)
    f.value:resource:category[0]:coding[0]:code::STRING                 AS category_code,
    f.value:resource:category[0]:coding[0]:display::STRING              AS category_display,

    -- Effective timing (QI-Core: when observation was performed)
    -- COALESCE handles effectiveDateTime vs effectivePeriod.start
    COALESCE(
        f.value:resource:effectiveDateTime::TIMESTAMP_TZ,
        f.value:resource:effectivePeriod:start::TIMESTAMP_TZ
    )                                                                   AS effective_datetime,

    -- Issued (when result was made available)
    f.value:resource:issued::TIMESTAMP_TZ                               AS issued_datetime,

    -- Value — numeric (valueQuantity for HbA1c, vitals, labs)
    f.value:resource:valueQuantity:value::FLOAT                         AS value_quantity,
    f.value:resource:valueQuantity:unit::STRING                         AS value_unit,
    f.value:resource:valueQuantity:system::STRING                       AS value_unit_system,
    f.value:resource:valueQuantity:code::STRING                         AS value_unit_code,

    -- Value — coded (valueCodeableConcept for findings)
    f.value:resource:valueCodeableConcept:coding[0]:code::STRING        AS value_concept_code,
    f.value:resource:valueCodeableConcept:coding[0]:display::STRING     AS value_concept_display,
    f.value:resource:valueCodeableConcept:coding[0]:system::STRING      AS value_concept_system,

    -- Value — string (valueString for free text results)
    f.value:resource:valueString::STRING                                AS value_string,

    -- Data absent reason (QI-Core — missing result = poor control for CMS122)
    f.value:resource:dataAbsentReason:coding[0]:code::STRING            AS data_absent_reason_code,
    f.value:resource:dataAbsentReason:coding[0]:display::STRING         AS data_absent_reason_display,

    -- Interpretation (normal/abnormal/critical)
    f.value:resource:interpretation[0]:coding[0]:code::STRING           AS interpretation_code,
    f.value:resource:interpretation[0]:coding[0]:display::STRING        AS interpretation_display,

    -- Reference range (normal range for lab results)
    f.value:resource:referenceRange[0]:low:value::FLOAT                 AS reference_range_low,
    f.value:resource:referenceRange[0]:high:value::FLOAT                AS reference_range_high,

    -- Convenience flag for CMS122 HbA1c identification
    CASE
        WHEN f.value:resource:code:coding[0]:system::STRING
             = 'http://loinc.org'
         AND f.value:resource:code:coding[0]:code::STRING
             IN ('4548-4', '17856-6', '59261-8')
        THEN TRUE
        ELSE FALSE
    END                                                                 AS is_hba1c,

    -- Lineage
    b.source_filename,
    b.datetime_loaded                                                   AS loaded_at

FROM {{ source('raw', 'bundle') }} b,
    LATERAL FLATTEN(input => b.raw_bundle:entry) f

WHERE f.value:resource:resourceType::STRING = 'Observation'