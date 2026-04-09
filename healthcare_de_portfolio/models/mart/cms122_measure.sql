{{
    config(
        materialized='table'
    )
}}

-- CMS122: Diabetes: Hemoglobin A1c (HbA1c) Poor Control (>9%)
-- Measurement period: 2024 calendar year
-- Higher rate = worse performance (inverse measure)
-- Value sets sourced from VSAC eCQM Update 2023-05-04 (2024 performance year)
-- TODO Project 3: Move measurement period to dbt variables

WITH measurement_period AS (
    SELECT
        '2024-01-01'::DATE AS period_start,
        '2024-12-31'::DATE AS period_end
),

-- Qualifying encounters during measurement period
qualifying_encounters AS (
    SELECT DISTINCT
        fe.patient_id
    FROM {{ ref('fact_encounter') }} fe
    CROSS JOIN measurement_period mp
    WHERE fe.encounter_start::DATE BETWEEN mp.period_start AND mp.period_end
        AND fe.encounter_class_code IN ('AMB', 'EMER', 'IMP', 'OBSENC')
        AND fe.status = 'finished'
),

-- Denominator: diabetic patients 18-75, alive, with qualifying encounter
denominator AS (
    SELECT
        p.patient_id,
        p.birth_date,
        p.gender,
        p.race,
        p.ethnicity,
        p.is_deceased
    FROM {{ ref('dim_patient') }} p
    CROSS JOIN measurement_period mp
    WHERE p.is_diabetic = TRUE
        AND DATEDIFF('year', p.birth_date, mp.period_end) BETWEEN 18 AND 75
        AND p.is_deceased = FALSE
        AND p.patient_id IN (SELECT patient_id FROM qualifying_encounters)
),

-- HbA1c observations during measurement period
-- Identified via VSAC value set OID 2.16.840.1.113883.3.464.1003.198.12.1013
hba1c_in_period AS (
    SELECT
        o.patient_id,
        o.observation_id,
        o.observation_code,
        o.value_quantity                                AS hba1c_value,
        o.effective_datetime                            AS hba1c_date,
        o.data_absent_reason_code,
        o.status
    FROM {{ ref('fact_observation') }} o
    INNER JOIN {{ ref('ref_code_system_map') }} csm
        ON csm.fhir_uri = o.observation_code_system
    INNER JOIN {{ ref('ref_value_sets') }} vs
        -- HbA1c Laboratory Test
        -- (VSAC OID 2.16.840.1.113883.3.464.1003.198.12.1013)
        ON vs.value_set_oid = '2.16.840.1.113883.3.464.1003.198.12.1013'
        AND vs.code = o.observation_code
        AND vs.code_system_oid = csm.vsac_oid
    CROSS JOIN measurement_period mp
    WHERE o.effective_datetime::DATE
            BETWEEN mp.period_start AND mp.period_end
        AND o.status = 'final'
),

-- Most recent HbA1c per patient during measurement period
most_recent_hba1c AS (
    SELECT
        patient_id,
        hba1c_value,
        hba1c_date,
        data_absent_reason_code,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY hba1c_date DESC
        )                                               AS rn
    FROM hba1c_in_period
),

-- Numerator: poor control or no result
numerator AS (
    SELECT
        d.patient_id,
        CASE
            WHEN h.patient_id IS NULL
                THEN 'NO_RESULT'
            WHEN h.hba1c_value > 9.0
                THEN 'POOR_CONTROL'
            WHEN h.hba1c_value IS NULL
             AND h.data_absent_reason_code IS NOT NULL
                THEN 'ABSENT_RESULT'
            ELSE NULL
        END                                             AS numerator_reason
    FROM denominator d
    LEFT JOIN most_recent_hba1c h
        ON d.patient_id = h.patient_id
        AND h.rn = 1
    WHERE h.patient_id IS NULL
       OR h.hba1c_value > 9.0
       OR (h.hba1c_value IS NULL
           AND h.data_absent_reason_code IS NOT NULL)
)

SELECT
    d.patient_id,
    d.birth_date,
    d.gender,
    d.race,
    d.ethnicity,
    mp.period_start                                     AS measurement_period_start,
    mp.period_end                                       AS measurement_period_end,
    TRUE                                                AS in_denominator,
    CASE WHEN n.patient_id IS NOT NULL
        THEN TRUE ELSE FALSE
    END                                                 AS in_numerator,
    n.numerator_reason,
    h.hba1c_value,
    h.hba1c_date,
    h.data_absent_reason_code,
    CASE WHEN n.patient_id IS NOT NULL
        THEN 'POOR_CONTROL' ELSE 'CONTROLLED'
    END                                                 AS measure_result

FROM denominator d
CROSS JOIN measurement_period mp
LEFT JOIN numerator n ON d.patient_id = n.patient_id
LEFT JOIN most_recent_hba1c h
    ON d.patient_id = h.patient_id
    AND h.rn = 1