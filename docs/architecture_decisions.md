# Architecture Decision Record
## Healthcare DE Portfolio — Project 1

**Author:** Kevin McCreery  
**Stack:** Snowflake, dbt Core, Python, Synthea FHIR R4  
**Date:** April 2026

---

## Project Overview

A healthcare data engineering portfolio demonstrating FHIR R4 ingestion, dimensional modeling, and CQM calculation using synthetic patient data. Three progressive projects building toward a production-grade pipeline with orchestration, data quality validation, IaC, and CI/CD.

**Data:** Synthea-generated synthetic FHIR R4 bundles, ~34,212 patients across Washington, Oregon, and California. No real PHI involved.

**Clinical focus:** Diabetic patient cohort. CMS122 (HbA1c Poor Control) and CMS111 (ED Arrival to Departure Time).

---

## ADR-001: Raw Layer Design — Single VARIANT Column per Bundle

**Decision:** Load each FHIR bundle as one row in `RAW.BUNDLE` with the full JSON stored in a Snowflake VARIANT column. All decomposition happens downstream in dbt.

**Alternatives considered:**
- Pre-parse bundles in Python before loading (rejected — moves compute to local machine, duplicates logic between Python and dbt, loses raw fidelity)
- Separate raw tables per resource type (rejected — requires parsing before landing, violates raw layer immutability principle)

**Rationale:** Snowflake's VARIANT type and LATERAL FLATTEN make decomposition efficient at query time. Landing raw bundles preserves the original data for reprocessing. All compute stays in Snowflake where it can scale independently of storage.

**Tradeoff:** Staging model queries are slower because LATERAL FLATTEN re-executes on every query. Mitigated by materializing mart models as tables.

---

## ADR-002: Three Purpose-Specific Stages

**Decision:** Three Snowflake internal stages — `FHIR_PATIENT_STAGE`, `FHIR_ORGANIZATION_STAGE`, `FHIR_PRACTITIONER_STAGE` — rather than one combined stage.

**Alternatives considered:**
- Single stage with PATTERN filtering in COPY INTO (rejected — conflates different resource types with different update frequencies and consumers)

**Rationale:** Mirrors real-world FHIR data contracts where different resource types arrive from different sources on different schedules. Organization and practitioner data changes rarely; patient bundles arrive continuously. Separating stages makes the pipeline's dependency order explicit and maps cleanly to FHIR R5 topic-based subscriptions (Project 2).

---

## ADR-003: Lightweight Load Manifest

**Decision:** `RAW.LOAD_MANIFEST` tracks batch-level operational metadata only — file count, total bytes, rows loaded/rejected, status, error message. No per-file tracking, no checksums.

**Alternatives considered:**
- Heavy manifest with per-file tracking and pre-extracted business metadata (rejected — duplicates what Snowflake's COPY_HISTORY already tracks, adds failure surface, violates single responsibility)
- No manifest at all (rejected — loses operational audit trail not captured by Snowflake natively)

**Rationale:** Snowflake's `INFORMATION_SCHEMA.COPY_HISTORY` already tracks per-file load status. The manifest owns what Snowflake doesn't: batch identity (`load_id`), business-level counts, and pipeline status for Airflow integration in Project 2. `load_id` will become the Airflow run ID in Project 2 — designed for that transition.

**Known limitation:** `datetime_received` represents load time, not true file arrival time. True arrival time would require a pre-load manifest write from the Python pipeline, which adds complexity not justified for this project.

---

## ADR-004: dbt Staging Models as Views, Mart Models as Tables

**Decision:** Staging models materialized as views, mart models as tables.

**Rationale:** Staging models are intermediate — they exist to feed mart models, not to be queried directly. Views have zero storage cost. Mart models are queried repeatedly by dashboards; materializing them pays for itself in query performance.

**Known limitation:** Staging view queries re-execute LATERAL FLATTEN across 34K bundles on every access. `stg_patient` specifically takes ~70 seconds to query because of nested extension handling. In production with higher data volumes, staging models should be converted to incremental tables.

**Exception:** `stg_patient` was initially attempted as a view but required table materialization during development due to a Snowflake correlated subquery limitation in views. This was later resolved by rewriting to comma-syntax LATERAL FLATTEN, and `stg_patient` was reverted to view materialization.

---

## ADR-005: Comma-Syntax LATERAL FLATTEN over Correlated Subqueries

**Decision:** All FHIR VARIANT navigation uses comma-syntax LATERAL FLATTEN in the FROM clause with `MAX(CASE WHEN ...)` aggregation for pivot operations. Correlated subqueries are not used.

**Rationale:** Snowflake does not support correlated subqueries that reference outer LATERAL FLATTEN aliases. This was discovered during `stg_patient` development. The comma-syntax pattern is Snowflake's native approach and performs well.

**Pattern established:**
```sql
-- Extracting values from nested FHIR extensions
FROM source_table b,
    LATERAL FLATTEN(input => b.raw_bundle:entry) f,
    LATERAL FLATTEN(input => f.value:resource:extension) ext
-- Pivot back to one row per patient
MAX(CASE WHEN ext.value:url::STRING LIKE '%us-core-birthsex%'
    THEN ext.value:valueCode::STRING END) AS birth_sex
GROUP BY [all non-aggregated columns]
```

---

## ADR-006: Patient Reference Parsing

**Decision:** Patient references in FHIR resources are parsed using `SPLIT_PART(..., 'urn:uuid:', 2)`. Organization and practitioner references use `SPLIT_PART(..., '|', 2)` to extract the identifier after the system pipe.

**Rationale:** Synthea uses `urn:uuid:` prefix for patient subject references and pipe-delimited system|value format for practitioner NPI references. This is consistent across all bundles in the dataset.

**Known limitation:** Real EHR systems may use different reference formats. Production implementation should use a macro that handles multiple reference formats gracefully.

---

## ADR-007: Diabetic Cohort Definition — Hardcoded Codes

**Decision:** Diabetic cohort currently defined using ICD-10 wildcards `E10%` (Type 1) and `E11%` (Type 2) plus five SNOMED codes.

**Known gap:** The official CMS122 diabetes value set (VSAC OID `2.16.840.1.113883.3.464.1003.103.12.1001`) contains 369 concepts including E08 (diabetes due to underlying condition), E09 (drug-induced diabetes), and E13 (other specified diabetes). These are excluded from the current implementation.

**Production fix:** Register at vsac.nlm.nih.gov, download the official value set CSV, load as a dbt seed (`seeds/ref_diabetes_codes.csv`), and update `dim_patient` and `fact_condition` to JOIN against the seed rather than hardcoded codes.

---

## ADR-008: is_superseded Not Implemented

**Decision:** `is_superseded` column exists in all raw tables but defaults to FALSE for all rows. No supersession logic is implemented.

**Rationale:** Synthea generates complete historical records in one shot. No patient bundle is updated or replaced in this dataset. Implementing supersession logic without a real incremental data source would be untested and potentially misleading.

**Production fix (Project 2):** The Python ingestion pipeline should be extended with a post-load task that:
1. Identifies newly loaded bundles for patients who already have records
2. Sets `is_superseded = TRUE` on prior bundles for those patients
3. All dbt staging models should add `WHERE b.is_superseded = FALSE`

TODO comments marking this are present in all five staging models.

---

## ADR-009: CQM Measurement Period as Hardcoded CTE

**Decision:** Measurement period (2024-01-01 to 2024-12-31) is defined as a hardcoded CTE in each CQM model.

**Production fix (Project 3):** Move to dbt variables:
```bash
dbt run --vars '{"measurement_year": 2024}'
```

Referenced in models as:
```sql
'{{ var("measurement_year") }}-01-01'::DATE AS period_start
```

---

## ADR-010: QI-Core Alignment

**Decision:** Staging models are partially aligned with QI-Core STU 6 profiles. Full alignment not implemented.

**What is aligned:**
- US Core Patient profile fields (race, ethnicity, birth sex via extensions)
- Encounter class, type, period, status
- Condition clinicalStatus and verificationStatus
- Observation effectiveDateTime (not issued) per QI-Core timing guidance
- MedicationRequest authoredOn per QI-Core
- Full CodeableConcept arrays preserved as `_json` VARIANT columns

**Known gaps:**
- VSAC value sets not loaded (see ADR-007)
- Encounter.diagnosis element not extracted
- No EMPI integration (single source, not needed for Synthea)
- CQM logic implemented in SQL rather than CQL

---

## ADR-011: Surrogate Keys Not Implemented

**Decision:** Source system UUIDs (Synthea-generated) used as primary keys throughout.

**Known gap:** In a multi-source production pipeline, source system identifiers are not globally unique. Two EHR systems could each have a resource with id `abc-123`. Surrogate keys generated via `dbt_utils.generate_surrogate_key(['source_filename', 'resource_id'])` should replace source IDs as primary keys.

**Production fix:** Install `dbt_utils` package and add surrogate key generation to all staging models.

---

## Known Technical Debt Summary

| Item | Location | Priority | Notes |
|---|---|---|---|
| VSAC value sets | dim_patient, fact_condition, cms122_measure | High | Requires VSAC account |
| is_superseded logic | All staging models, ingestion pipeline | High | Project 2 |
| Surrogate keys | All staging and mart models | Medium | dbt_utils |
| Staging model performance | stg_* views | Medium | Convert to incremental tables at scale |
| Measurement period parameterization | cms122_measure, cms111_measure | Low | Project 3 |
| EMPI integration | stg_patient | Low | Multi-source only |
| CQL-based measure validation | cms122_measure, cms111_measure | Low | Validate against Bonnie |

---

## CMS122 Data Quality Finding

Pipeline validation confirmed that the 3.9% poor control rate produced by this pipeline against Synthea data is an artifact of Synthea's synthetic HbA1c distribution, not a pipeline logic error. Synthea generates HbA1c values clustered around 5.2% mean — physiologically implausible for a diabetic population (expected mean ~7.5%). With a realistic HbA1c distribution (NHANES-based), the same pipeline logic produces ~28-32% poor control, consistent with the CMS 2024 national benchmark of 27.3%.

See `notebooks/cms122_data_quality_analysis.ipynb` for full analysis.

---

## Repository Structure

```
healthcare-de-portfolio/
├── pipelines/
│   └── fhir_ingestion.py          ← Python ingestion pipeline
├── scripts/
│   └── dev_put_files.py           ← Dev utility: PUT files to Snowflake stage
├── snowflake/
│   └── ddl/
│       ├── raw_tables.sql         ← RAW schema DDL
│       └── stages.sql             ← Stage and file format DDL
├── notebooks/
│   └── cms122_data_quality_analysis.ipynb
├── healthcare_de_portfolio/       ← dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_patient.sql
│   │   │   ├── stg_encounter.sql
│   │   │   ├── stg_condition.sql
│   │   │   ├── stg_observation.sql
│   │   │   └── stg_medication_request.sql
│   │   └── mart/
│   │       ├── schema.yml
│   │       ├── dim_patient.sql
│   │       ├── fact_encounter.sql
│   │       ├── fact_condition.sql
│   │       ├── fact_observation.sql
│   │       ├── fact_medication_request.sql
│   │       ├── cms122_measure.sql
│   │       └── cms111_measure.sql
│   └── dbt_project.yml
└── docs/
    └── architecture_decisions.md  ← this file
```
