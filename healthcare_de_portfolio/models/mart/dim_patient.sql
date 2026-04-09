{{
    config(
        materialized='table'
    )
}}

-- Diabetic patient cohort dimension
-- Diabetes cohort sourced from VSAC value set OID 2.16.840.1.113883.3.464.1003.103.12.1001
-- Code system mapping via ref_code_system_map (FHIR URI → VSAC OID)

WITH diabetic_patients AS (
    SELECT DISTINCT
        c.patient_id
    FROM {{ ref('stg_condition') }} c
    INNER JOIN {{ ref('ref_code_system_map') }} csm
        ON csm.fhir_uri = c.condition_code_system
    INNER JOIN {{ ref('ref_value_sets') }} vs
        -- Diabetes (VSAC OID 2.16.840.1.113883.3.464.1003.103.12.1001)
        ON vs.value_set_oid = '2.16.840.1.113883.3.464.1003.103.12.1001'
        AND vs.code = c.condition_code
        AND vs.code_system_oid = csm.vsac_oid
    WHERE c.is_active_confirmed = TRUE
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
    -- Sourced from VSAC Diabetes value set, not hardcoded codes
    CASE WHEN d.patient_id IS NOT NULL
        THEN TRUE ELSE FALSE
    END                                     AS is_diabetic,

    -- Lineage
    p.source_filename,
    p.loaded_at

FROM {{ ref('stg_patient') }} p
LEFT JOIN diabetic_patients d ON p.patient_id = d.patient_id