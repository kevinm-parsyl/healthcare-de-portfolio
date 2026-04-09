{{
    config(
        materialized='table'
    )
}}

SELECT
    c.condition_id,
    c.patient_id,
    c.clinical_status,
    c.verification_status,
    c.condition_code,
    c.condition_display,
    c.condition_code_system,
    c.condition_text,
    c.condition_codings_json,
    c.onset_datetime,
    c.abatement_datetime,
    c.recorded_date,
    c.category_code,
    c.category_display,
    c.encounter_id,
    c.is_active_confirmed,
    c.codings_json,
    c.categories_json,
    c.source_filename,
    c.loaded_at,

    -- Patient context
    p.birth_date,
    p.gender,
    p.race,
    p.ethnicity,
    p.is_diabetic

FROM {{ ref('stg_condition') }} c
LEFT JOIN {{ ref('dim_patient') }} p
    ON c.patient_id = p.patient_id