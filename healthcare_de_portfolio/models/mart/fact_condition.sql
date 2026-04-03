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
    p.is_diabetic,

    -- Diabetes flag on this specific condition
    CASE
        WHEN c.condition_code_system = 'http://snomed.info/sct'
         AND c.condition_code IN (
             '44054006', '73211009', '46635009',
             '31321000119102', '368581000119106'
         )
        THEN TRUE
        WHEN c.condition_code_system = 'http://hl7.org/fhir/sid/icd-10-cm'
         AND (c.condition_code LIKE 'E10%' OR c.condition_code LIKE 'E11%')
        THEN TRUE
        ELSE FALSE
    END                                                 AS is_diabetes_condition

FROM {{ ref('stg_condition') }} c
LEFT JOIN {{ ref('dim_patient') }} p
    ON c.patient_id = p.patient_id