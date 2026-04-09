# Architecture Decision Record
## Healthcare DE Portfolio — Project 1

**Author:** Kevin McCreery
**Stack:** Snowflake, dbt Core, Python, Synthea FHIR R4
**Last updated:** April 2026

---

## Project Overview

A healthcare data engineering portfolio demonstrating FHIR R4 ingestion, dimensional modeling, and CQM calculation using synthetic patient data. Three progressive projects building toward a production-grade pipeline with orchestration, data quality validation, IaC, and CI/CD.

**Data:** Synthea-generated synthetic FHIR R4 bundles, ~34,212 patients across Washington, Oregon, and California. No real PHI involved.

**Clinical focus:** Diabetic patient cohort. CMS122 (HbA1c Poor Control) and CMS111 (ED Arrival to Departure Time).

---

## ADR-001: Raw Layer Design — Single VARIANT Column per Bundle

**Decision:** Load each FHIR bundle as one row in `RAW.BUNDLE` with the full JSON stored in a Snowflake VARIANT column. All decomposition happens downstream in dbt.

**Alternatives considered:**
- Pre-parse bundles in Python before loading (rejected — moves compute to local machine, duplicates logic, loses raw fidelity)
- Separate raw tables per resource type (rejected — requires parsing before landing, violates raw layer immutability)

**Rationale:** Snowflake's VARIANT type and LATERAL FLATTEN make decomposition efficient at query time. Landing raw bundles preserves the original data for reprocessing. All compute stays in Snowflake where it scales independently of storage.

**Tradeoff:** Staging model queries re-execute LATERAL FLATTEN on every access. Mitigated by materializing mart models as tables.

---

## ADR-002: Three Purpose-Specific Stages

**Decision:** Three Snowflake internal stages — `FHIR_PATIENT_STAGE`, `FHIR_ORGANIZATION_STAGE`, `FHIR_PRACTITIONER_STAGE`.

**Rationale:** Mirrors real-world FHIR data contracts. Different resource types have different update frequencies and consumers. Maps cleanly to FHIR R5 topic-based subscriptions (Project 2).

---

## ADR-003: Lightweight Load Manifest

**Decision:** `RAW.LOAD_MANIFEST` tracks batch-level operational metadata only. No per-file tracking, no checksums.

**Rationale:** Snowflake's `INFORMATION_SCHEMA.COPY_HISTORY` already tracks per-file load status. Manifest owns what Snowflake doesn't: batch identity, business-level counts, pipeline status. `load_id` designed to become Airflow run ID in Project 2.

**Known limitation:** `datetime_received` represents load time, not true file arrival time.

---

## ADR-004: dbt Staging Views, Mart Tables

**Decision:** Staging models as views, mart models as tables.

**Rationale:** Staging models feed mart models, not dashboards. Views have zero storage cost. Mart models queried repeatedly by Looker Studio — materialization pays for itself.

**Known limitation:** stg_patient takes ~70s to query due to LATERAL FLATTEN re-execution. At scale, convert to incremental tables.

---

## ADR-005: Comma-Syntax LATERAL FLATTEN

**Decision:** All FHIR VARIANT navigation uses comma-syntax LATERAL FLATTEN. Correlated subqueries not used.

**Rationale:** Snowflake does not support correlated subqueries referencing outer LATERAL FLATTEN aliases. Discovered during stg_patient development.

**Pattern:**
```sql
FROM source_table b,
    LATERAL FLATTEN(input => b.raw_bundle:entry) f,
    LATERAL FLATTEN(input => f.value:resource:extension) ext
MAX(CASE WHEN ext.value:url::STRING LIKE '%us-core-birthsex%'
    THEN ext.value:valueCode::STRING END) AS birth_sex
GROUP BY [all non-aggregated columns]
```

---

## ADR-006: FHIR Reference Parsing

**Decision:** Patient references parsed via `SPLIT_PART(..., 'urn:uuid:', 2)`. Practitioner/org references via `SPLIT_PART(..., '|', 2)`.

**Known limitation:** Real EHR systems may use different reference formats. Production should use a macro handling multiple formats.

---

## ADR-007: VSAC Value Sets as dbt Seeds

**Decision:** Clinical terminology sourced from VSAC and loaded as dbt seeds.

**Seeds:**
- `ref_value_sets` — full CMS eCQM download, May 2023 release (2024 performance year), 105,272 rows
- `ref_code_system_map` — maps FHIR URI identifiers to VSAC OIDs (6 rows)

**Value sets used:**

| Measure | Value Set Name | VSAC OID | Codes |
|---|---|---|---|
| CMS122 | Diabetes | 2.16.840.1.113883.3.464.1003.103.12.1001 | 370 |
| CMS122 | HbA1c Laboratory Test | 2.16.840.1.113883.3.464.1003.198.12.1013 | 5 |
| CMS111 | Emergency Department E&M Visit | 2.16.840.1.113883.3.464.1003.101.12.1010 | 6 |

**Filtering:** Models filter by `value_set_oid` (not name) for accuracy. OID comments provide human readability.

**Validation:** Singular dbt test `assert_value_set_name_oid_one_to_one.sql` validates 1:1 relationship between value set name and OID.

**Code system mapping:** FHIR uses URI-format (`http://snomed.info/sct`). VSAC uses OID-format (`2.16.840.1.113883.6.96`). `ref_code_system_map` bridges this gap.

**Versioning:** CMS publishes annually. May 2023 release governs 2024 performance year. To update: replace CSV, run `dbt seed`.

**Synthea finding:** Expanded value set (370 codes vs 7 prior) did not change diabetic patient counts. Synthea uses high-level codes already covered. Real EHR data uses granular ICD-10 subcategories the expanded set captures.

---

## ADR-008: Measure-Specific Logic in CQM Models Only

**Decision:** Clinical quality measure logic (value set membership, HbA1c thresholds, ED identification) lives in CQM models, not fact tables.

**Removed from fact tables:**
- `fact_observation.is_hba1c` — CMS122-specific, now in cms122_measure CTE
- `fact_observation.is_hba1c_poor_control` — CMS122 business logic (>9%), now in cms122_measure
- `fact_condition.is_diabetes_condition` — CMS122-specific, dim_patient.is_diabetic serves this role
- `fact_encounter.is_ed_encounter_vsac` — CMS111-specific, now in cms111_measure

**Kept on fact/dim tables (defensible as general-purpose):**
- `dim_patient.is_diabetic` — useful across care management, risk stratification, population health
- `fact_encounter.is_ed_encounter` — class-based (EMER), describes encounter type
- `fact_encounter.is_inpatient_encounter` — same
- `fact_condition.is_active_confirmed` — clinically meaningful across all measures

---

## ADR-009: is_superseded Not Implemented

**Decision:** Column exists, defaults FALSE. No supersession logic implemented.

**Production fix (Project 2):** Post-load Airflow task sets `is_superseded = TRUE` on prior bundles for patients with new loads. All staging models add `WHERE b.is_superseded = FALSE`.

---

## ADR-010: Measurement Period Hardcoded

**Decision:** 2024-01-01 to 2024-12-31 hardcoded as CTEs in CQM models.

**Production fix (Project 3):** dbt variables — `dbt run --vars '{"measurement_year": 2024}'`

---

## ADR-011: QI-Core Partial Alignment

**Aligned:** US Core Patient extensions, Encounter class/type/period/status, Condition clinicalStatus + verificationStatus, Observation effectiveDateTime, MedicationRequest authoredOn, full CodeableConcept arrays as VARIANT.

**Gaps:** Encounter.diagnosis element, no EMPI, CQL not implemented (SQL used instead — validate against Bonnie for production).

---

## ADR-012: Surrogate Keys Not Implemented

Synthea UUIDs used as PKs. Production requires `dbt_utils.generate_surrogate_key()` for multi-source environments.

---

## Known Technical Debt

| Item | Location | Priority | Project |
|---|---|---|---|
| is_superseded logic | Staging models, ingestion pipeline | High | 2 |
| Surrogate keys | All models | Medium | 2 |
| Staging model performance | stg_* views | Medium | 2+ |
| Measurement period parameterization | CQM models | Low | 3 |
| EMPI integration | stg_patient | Low | Multi-source |
| CQL validation (Bonnie) | CQM models | Low | Future |

---

## CMS122 Data Quality Finding

Pipeline logic confirmed correct. 3.9% poor control rate is a Synthea data fidelity artifact — synthetic HbA1c values cluster around 5.2% mean vs expected 7.5% for a diabetic population. With realistic distributions, the same pipeline produces ~28-32%, consistent with CMS 2024 national benchmark of 27.3%.

See `notebooks/cms122_data_quality_analysis.ipynb` for full analysis.

---

## Repository Structure

```
healthcare-de-portfolio/
├── pipelines/fhir_ingestion.py
├── scripts/dev_put_files.py
├── snowflake/ddl/
│   ├── raw_tables.sql
│   └── stages.sql
├── notebooks/cms122_data_quality_analysis.ipynb
├── docs/architecture_decisions.md
└── healthcare_de_portfolio/
    ├── seeds/
    │   ├── ref_value_sets.csv
    │   └── ref_code_system_map.csv
    ├── tests/assert_value_set_name_oid_one_to_one.sql
    ├── models/
    │   ├── staging/ (5 models + sources.yml + schema.yml)
    │   └── mart/ (5 fact/dim + 2 CQM + schema.yml)
    └── dbt_project.yml
```
