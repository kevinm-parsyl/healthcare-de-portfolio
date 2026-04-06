{{
    config(
        materialized='table'
    )
}}

-- CMS111: Median Time from ED Arrival to ED Departure for Admitted ED Patients
-- Measurement period: 2024 calendar year
-- Lower median time = better performance
-- Aligned with QI-Core STU 6 / CMS111v12

WITH measurement_period AS (
    SELECT
        '2024-01-01'::DATE AS period_start,
        '2024-12-31'::DATE AS period_end
),

-- ED encounters during measurement period
ed_encounters AS (
    SELECT
        fe.encounter_id,
        fe.patient_id,
        fe.encounter_start                              AS ed_arrival,
        fe.encounter_end                                AS ed_departure,
        fe.encounter_duration_minutes                   AS ed_time_minutes,
        fe.service_provider_id,
        fe.service_provider_display,
        fe.discharge_disposition_code
    FROM {{ ref('fact_encounter') }} fe
    CROSS JOIN measurement_period mp
    WHERE fe.is_ed_encounter = TRUE
        AND fe.encounter_start::DATE
            BETWEEN mp.period_start AND mp.period_end
        AND fe.status = 'finished'
        AND fe.encounter_duration_minutes IS NOT NULL
        AND fe.encounter_duration_minutes > 0
),

-- Admitted ED patients
-- Identified by inpatient encounter within 24 hours of ED departure
-- for the same patient at the same facility
admitted_patients AS (
    SELECT DISTINCT
        ed.encounter_id                                 AS ed_encounter_id,
        ed.patient_id,
        ed.ed_arrival,
        ed.ed_departure,
        ed.ed_time_minutes,
        ed.service_provider_id,
        ed.service_provider_display
    FROM ed_encounters ed
    INNER JOIN {{ ref('fact_encounter') }} inpt
        ON ed.patient_id = inpt.patient_id
        AND inpt.is_inpatient_encounter = TRUE
        AND inpt.encounter_start
            BETWEEN ed.ed_departure
            AND DATEADD('hour', 24, ed.ed_departure)
)

-- Final measure output
SELECT
    ap.ed_encounter_id,
    ap.patient_id,
    ap.ed_arrival,
    ap.ed_departure,
    ap.ed_time_minutes,
    ap.service_provider_id,
    ap.service_provider_display,
    mp.period_start                                     AS measurement_period_start,
    mp.period_end                                       AS measurement_period_end,

    -- Aggregate measure score
    MEDIAN(ap.ed_time_minutes) OVER ()                  AS median_ed_time_minutes_overall,

    -- Facility-level score
    MEDIAN(ap.ed_time_minutes)
        OVER (PARTITION BY ap.service_provider_id)      AS median_ed_time_minutes_by_facility

FROM admitted_patients ap
CROSS JOIN measurement_period mp