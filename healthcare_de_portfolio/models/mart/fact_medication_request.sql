{{
    config(
        materialized='table'
    )
}}

SELECT
    m.medication_request_id,
    m.patient_id,
    m.encounter_id,
    m.status,
    m.intent,
    m.medication_code,
    m.medication_display,
    m.medication_code_system,
    m.medication_text,
    m.medication_codings_json,
    m.authored_on,
    m.requester_npi,
    m.requester_display,
    m.dosage_text,
    m.dosage_frequency,
    m.dosage_period,
    m.dosage_period_unit,
    m.dose_quantity_value,
    m.dose_quantity_unit,
    m.refills_allowed,
    m.dispense_quantity,
    m.dispense_quantity_unit,
    m.validity_start,
    m.validity_end,
    m.reason_code,
    m.reason_display,
    m.reason_system,
    m.dosage_instructions_json,
    m.reason_codes_json,
    m.source_filename,
    m.loaded_at,

    -- Patient context
    p.birth_date,
    p.gender,
    p.is_diabetic

FROM {{ ref('stg_medication_request') }} m
LEFT JOIN {{ ref('dim_patient') }} p
    ON m.patient_id = p.patient_id