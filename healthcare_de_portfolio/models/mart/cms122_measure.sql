{{
    config(
        materialized='table'
    )
}}

-- CMS122: Diabetes: Hemoglobin A1c (HbA1c) Poor Control (>9%)
-- Measurement period: 2024 calendar year
-- Higher rate = worse performance (inverse measure)
-- Aligned with QI-Core STU 6 / CMS122v12

-- Measurement period configuration
-- TODO Project 3: Move to dbt variables for parameterization
--     dbt run --vars '{"measurement_year": 2024}'

WITH measurement_period AS (
    SELECT
        '2024-01-01'::DATE AS period_start,
        '2024-12-31'::DATE AS period_end
),

-- Initial population and denominator:
-- Diabetic patients aged 18-75 with qualifying encounter during measurement period
qualifying_encounters AS (
    SELECT DISTINCT
        fe.patient_id
    FROM {{ ref('fact_encounter') }} fe
    CROSS JOIN measurement_period mp
    WHERE fe.encounter_start::DATE BETWEEN mp.period_start AND mp.period_end
        AND fe.encounter_class_code IN ('AMB', 'EMER', 'IMP', 'OBSENC')
        AND fe.status = 'finished'
),

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

-- Most recent HbA1c result per patient during measurement period
most_recent_hba1c AS (
    SELECT
        fo.patient_id,
        fo.value_quantity                               AS hba1c_value,
        fo.effective_datetime                           AS hba1c_date,
        fo.data_absent_reason_code,
        fo.is_hba1c_poor_control,
        ROW_NUMBER() OVER (
            PARTITION BY fo.patient_id
            ORDER BY fo.effective_datetime DESC
        )                                               AS rn
    FROM {{ ref('fact_observation') }} fo
    CROSS JOIN measurement_period mp
    WHERE fo.is_hba1c = TRUE
        AND fo.effective_datetime::DATE
            BETWEEN mp.period_start AND mp.period_end
        AND fo.status = 'final'
),

-- Numerator:
-- Patients whose most recent HbA1c > 9% OR no HbA1c result during period
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
    WHERE h.patient_id IS NULL          -- no HbA1c result
       OR h.hba1c_value > 9.0          -- poor control
       OR (h.hba1c_value IS NULL
           AND h.data_absent_reason_code IS NOT NULL)  -- absent result
)

-- Final measure output — one row per patient in denominator
SELECT
    d.patient_id,
    d.birth_date,
    d.gender,
    d.race,
    d.ethnicity,
    mp.period_start                                     AS measurement_period_start,
    mp.period_end                                       AS measurement_period_end,

    -- Denominator flag
    TRUE                                                AS in_denominator,

    -- Numerator flag
    CASE WHEN n.patient_id IS NOT NULL
        THEN TRUE ELSE FALSE
    END                                                 AS in_numerator,

    -- Numerator reason for auditability
    n.numerator_reason,

    -- Most recent HbA1c details
    h.hba1c_value,
    h.hba1c_date,
    h.data_absent_reason_code,

    -- Measure result per patient
    CASE WHEN n.patient_id IS NOT NULL
        THEN 'POOR_CONTROL' ELSE 'CONTROLLED'
    END                                                 AS measure_result

FROM denominator d
CROSS JOIN measurement_period mp
LEFT JOIN numerator n ON d.patient_id = n.patient_id
LEFT JOIN most_recent_hba1c h
    ON d.patient_id = h.patient_id
    AND h.rn = 1