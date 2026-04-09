-- tests/assert_value_set_name_oid_one_to_one.sql
-- Fails if any value_set_name maps to more than one value_set_oid,
-- or any value_set_oid maps to more than one value_set_name.
-- A returned row indicates a broken 1:1 relationship.

WITH name_to_oid AS (
    SELECT
        value_set_name,
        COUNT(DISTINCT value_set_oid) AS oid_count
    FROM {{ ref('ref_value_sets') }}
    GROUP BY value_set_name
    HAVING COUNT(DISTINCT value_set_oid) > 1
),

oid_to_name AS (
    SELECT
        value_set_oid,
        COUNT(DISTINCT value_set_name) AS name_count
    FROM {{ ref('ref_value_sets') }}
    GROUP BY value_set_oid
    HAVING COUNT(DISTINCT value_set_name) > 1
)

SELECT 'name_maps_to_multiple_oids' AS violation_type,
       value_set_name AS value,
       oid_count AS cardinality
FROM name_to_oid

UNION ALL

SELECT 'oid_maps_to_multiple_names' AS violation_type,
       value_set_oid AS value,
       name_count AS cardinality
FROM oid_to_name