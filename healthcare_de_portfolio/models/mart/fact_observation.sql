{{
    config(
        materialized='table'
    )
}}

SELECT
    o.observation_id,
    o.patient_id,
    o.encounter_id,
    o.status,
    o.observation_code,
    o.observation_display,
    o.observation_code_system,
    o.observation_text,
    o.observation_codings_json,
    o.category_code,
    o.category_display,
    o.effective_datetime,
    o.issued_datetime,
    o.value_quantity,
    o.value_unit,
    o.value_unit_system,
    o.value_unit_code,
    o.value_concept_code,
    o.value_concept_display,
    o.value_concept_system,
    o.value_string,
    o.data_absent_reason_code,
    o.data_absent_reason_display,
    o.interpretation_code,
    o.interpretation_display,
    o.reference_range_low,
    o.reference_range_high,
    o.is_hba1c,
    o.source_filename,
    o.loaded_at,

    -- Patient context
    p.birth_date,
    p.gender,
    p.is_diabetic,

    -- HbA1c poor control flag (>9% per CMS122)
    CASE
        WHEN o.is_hba1c = TRUE
         AND o.value_quantity > 9.0
        THEN TRUE
        WHEN o.is_hba1c = TRUE
         AND o.value_quantity IS NULL
         AND o.data_absent_reason_code IS NOT NULL
        THEN TRUE  -- missing result = poor control per CMS122
        ELSE FALSE
    END                                                 AS is_hba1c_poor_control

FROM {{ ref('stg_observation') }} o
LEFT JOIN {{ ref('dim_patient') }} p
    ON o.patient_id = p.patient_id