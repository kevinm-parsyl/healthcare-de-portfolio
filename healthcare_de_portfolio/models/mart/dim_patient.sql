{{
    config(
        materialized='table'
    )
}}

-- Diabetic patient cohort dimension
-- Includes all patients with at least one active confirmed diabetes diagnosis
-- Diabetes identified by SNOMED or ICD-10 codes per CMS122 value sets

WITH diabetic_patients AS (
    SELECT DISTINCT
        patient_id
    FROM {{ ref('stg_condition') }}
    WHERE is_active_confirmed = TRUE
        AND (
            -- SNOMED diabetes codes
            (condition_code_system = 'http://snomed.info/sct'
             AND condition_code IN (
                '44054006',  -- Type 2 diabetes mellitus
                '73211009',  -- Diabetes mellitus
                '46635009',  -- Type 1 diabetes mellitus
                '31321000119102', -- Type 2 in obesity
                '368581000119106' -- Type 2 with hyperglycemia
             ))
            OR
            -- ICD-10 diabetes codes (E10* = Type 1, E11* = Type 2)
            (condition_code_system = 'http://hl7.org/fhir/sid/icd-10-cm'
             AND (
                 condition_code LIKE 'E10%'
                 OR condition_code LIKE 'E11%'
             ))
        )
)

SELECT
    p.patient_id,
    p.medical_record_number,
    p.first_name,
    p.last_name,
    p.name_prefix,
    p.birth_date,
    p.gender,
    p.birth_sex,
    p.race,
    p.ethnicity,
    p.primary_language_code,
    p.marital_status_code,
    p.marital_status_display,
    p.is_deceased,
    p.deceased_datetime,
    p.is_multiple_birth,
    p.address_line_1,
    p.city,
    p.state,
    p.postal_code,
    p.country,
    p.latitude,
    p.longitude,
    p.phone,
    p.fhir_profile,
    p.identifiers_json,

    -- Diabetic cohort flag
    CASE WHEN d.patient_id IS NOT NULL
        THEN TRUE ELSE FALSE
    END                                     AS is_diabetic,

    -- Lineage
    p.source_filename,
    p.loaded_at

FROM {{ ref('stg_patient') }} p
LEFT JOIN diabetic_patients d ON p.patient_id = d.patient_id