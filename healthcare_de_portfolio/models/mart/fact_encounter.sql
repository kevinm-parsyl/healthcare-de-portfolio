{{
    config(
        materialized='table'
    )
}}

SELECT
    e.encounter_id,
    e.patient_id,
    e.status,
    e.encounter_class_code,
    e.encounter_class_system,
    e.type_code,
    e.type_display,
    e.type_system,
    e.type_codings_json,
    e.encounter_start,
    e.encounter_end,
    e.encounter_duration_minutes,
    e.reason_code,
    e.reason_display,
    e.reason_system,
    e.practitioner_npi,
    e.practitioner_display,
    e.service_provider_id,
    e.service_provider_display,
    e.location_display,
    e.discharge_disposition_code,
    e.discharge_disposition_display,
    e.primary_diagnosis_reference,
    e.primary_diagnosis_use_code,
    e.is_ed_encounter,
    e.is_inpatient_encounter,
    e.types_json,
    e.reason_codes_json,
    e.participants_json,
    e.locations_json,
    e.diagnoses_json,
    e.source_filename,
    e.loaded_at,

    -- Patient context
    p.birth_date,
    p.gender,
    p.race,
    p.ethnicity,
    p.is_diabetic,
    p.is_deceased,

    -- Age at time of encounter
    DATEDIFF('year', p.birth_date, e.encounter_start)   AS age_at_encounter

FROM {{ ref('stg_encounter') }} e
LEFT JOIN {{ ref('dim_patient') }} p
    ON e.patient_id = p.patient_id