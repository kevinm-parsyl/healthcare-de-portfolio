{{
    config(
        materialized='view'
    )
}}

-- TODO Project 2: Add WHERE b.is_superseded = FALSE filter once
-- supersession logic is implemented in ingestion pipeline.
-- Staging model aligned with QI-Core STU 6 MedicationRequest profile.
-- authoredOn used per QI-Core for prescription date.

SELECT
    -- MedicationRequest identity
    f.value:resource:id::STRING                                         AS medication_request_id,
    SPLIT_PART(
        f.value:resource:subject:reference::STRING, 'urn:uuid:', 2
    )                                                                   AS patient_id,
    SPLIT_PART(
        f.value:resource:encounter:reference::STRING, 'urn:uuid:', 2
    )                                                                   AS encounter_id,

    -- Status and intent
    f.value:resource:status::STRING                                     AS status,
    f.value:resource:intent::STRING                                     AS intent,

    -- Medication code — Synthea uses medicationCodeableConcept (RxNorm)
    f.value:resource:medicationCodeableConcept:coding[0]:code::STRING   AS medication_code,
    f.value:resource:medicationCodeableConcept:coding[0]:display::STRING AS medication_display,
    f.value:resource:medicationCodeableConcept:coding[0]:system::STRING AS medication_code_system,
    f.value:resource:medicationCodeableConcept:text::STRING             AS medication_text,
    f.value:resource:medicationCodeableConcept:coding                   AS medication_codings_json,

    -- Timing (QI-Core: authoredOn = when prescription was written)
    f.value:resource:authoredOn::TIMESTAMP_TZ                          AS authored_on,

    -- Requester (prescribing provider)
    SPLIT_PART(
        f.value:resource:requester:reference::STRING, '|', 2
    )                                                                   AS requester_npi,
    f.value:resource:requester:display::STRING                          AS requester_display,

    -- Dosage instructions
    f.value:resource:dosageInstruction[0]:text::STRING                  AS dosage_text,
    f.value:resource:dosageInstruction[0]:timing:repeat:frequency::INT  AS dosage_frequency,
    f.value:resource:dosageInstruction[0]:timing:repeat:period::FLOAT   AS dosage_period,
    f.value:resource:dosageInstruction[0]:timing:repeat:periodUnit::STRING AS dosage_period_unit,
    f.value:resource:dosageInstruction[0]:doseAndRate[0]:doseQuantity:value::FLOAT AS dose_quantity_value,
    f.value:resource:dosageInstruction[0]:doseAndRate[0]:doseQuantity:unit::STRING AS dose_quantity_unit,

    -- Dispense request
    f.value:resource:dispenseRequest:numberOfRepeatsAllowed::INT        AS refills_allowed,
    f.value:resource:dispenseRequest:quantity:value::FLOAT              AS dispense_quantity,
    f.value:resource:dispenseRequest:quantity:unit::STRING              AS dispense_quantity_unit,
    f.value:resource:dispenseRequest:validityPeriod:start::TIMESTAMP_TZ AS validity_start,
    f.value:resource:dispenseRequest:validityPeriod:end::TIMESTAMP_TZ   AS validity_end,

    -- Reason (why medication was prescribed)
    f.value:resource:reasonCode[0]:coding[0]:code::STRING               AS reason_code,
    f.value:resource:reasonCode[0]:coding[0]:display::STRING            AS reason_display,
    f.value:resource:reasonCode[0]:coding[0]:system::STRING             AS reason_system,

    -- Preserved arrays
    f.value:resource:dosageInstruction                                  AS dosage_instructions_json,
    f.value:resource:reasonCode                                         AS reason_codes_json,

    -- Lineage
    b.source_filename,
    b.datetime_loaded                                                   AS loaded_at

FROM {{ source('raw', 'bundle') }} b,
    LATERAL FLATTEN(input => b.raw_bundle:entry) f

WHERE f.value:resource:resourceType::STRING = 'MedicationRequest'