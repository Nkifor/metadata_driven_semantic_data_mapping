# Metadata-Driven Semantic Data Mapping & Entity Resolution (Workload Repo)

Application and workload repository for **GDPR-aware Semantic Data Mapping & Entity Resolution** on Databricks. 

This repository contains the Databricks-side workload code, including PySpark jobs, Python domain logic, Databricks Asset Bundle (DAB) configurations, sample data, and local/CI validation workflows.

> 
> This repository (**Repo nr 3**) does not provision Azure or Databricks infrastructure. It consumes Unity Catalog objects and external volumes created and managed by:
> 1. `data-mapping-bootstrap` (Repo nr 1)
> 2. `data-mapping-infra` (Repo nr 2)

---

## Current Implementation Status

The pipeline currently implements stages `00` through `05`. Stages `06` through `08` are planned for future iterations.

### Executed Data Flow
```text
Sample CSV Files
   │
   ▼
Unity Catalog External Volumes
   │
   ▼
Bronze Raw Delta Tables (01_ingest)
   │
   ▼
Source Profiling (02_profile_sources)
   │
   ▼
Semantic Mapping (03_semantic_mapping)
   │
   ▼
Silver Standardized & Pseudonymized Records (04_standardize_and_mask)
   │
   ▼
Compliance Eligibility & Suppression Events (05_apply_compliance_controls)
```

### Next Stages (Backlog)

- `06_entity_resolution`
- `07_quality_metrics`
- `08_build_gold_marts`
- Databricks SQL Dashboard integration
- Wheel packaging for application logic
- CI/CD hardening

---

## External Contract & Architecture

Workloads strictly use Unity Catalog `/Volumes/...` paths. They do not use SAS tokens, storage account keys, or legacy DBFS mounts.

### Expected Catalog & Schemas

| Item | Value |
|---|---|
| **Target Catalog** | `mapping_demo_dev` |
| **Target Schemas** | `bronze`, `silver`, `gold`, `metadata`, `compliance` |

### Expected External Volumes

```text
/Volumes/mapping_demo_dev/bronze/crm_landing_ext/
/Volumes/mapping_demo_dev/bronze/erp_landing_ext/
/Volumes/mapping_demo_dev/bronze/reference_landing_ext/
/Volumes/mapping_demo_dev/bronze/compliance_landing_ext/
/Volumes/mapping_demo_dev/bronze/checkpoints_ext/
/Volumes/mapping_demo_dev/bronze/archive_ext/
/Volumes/mapping_demo_dev/bronze/rejected_ext/
```

### Repository Structure

```text
data-mapping-databricks/
├── .github/workflows/     # CI/CD workflow definitions
├── jobs/                  # Databricks job entrypoints
├── sample_data/           # Local sample CSV files for testing
├── src/
│   └── mapping_engine/    # Reusable Python and PySpark domain logic
├── sql/validation/        # Manual SQL checks for post-pipeline verification
├── tests/                 # Local unit tests for pure Python logic
└── databricks.yml         # Databricks Asset Bundle configuration source of truth
```

---

## Local Development Setup

### 1. Environment Isolation

Create and activate a Python 3.11 virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

### 2. Dependency Installation

Install the project in editable mode along with development dependencies:

```bash
python -m pip install --upgrade pip setuptools wheel
pip install -e ".[dev]"
```

### 3. Verification

Run local unit tests and code linter to verify the configuration:

```bash
pytest
ruff check .
```

---

## Environment Configuration

Copy the example environment template to create your local configuration:

```bash
cp .env.example .env
```

> [!WARNING]
> `.env` and any `.env.*` files are local-only and must not be committed to Git. Never commit Databricks tokens, Azure credentials, workspace secrets, or production salts.

### Expected `.env` Shape

```ini
DATABRICKS_HOST=https://<your-workspace-url>.azuredatabricks.net
DATABRICKS_PROFILE=DEFAULT

APP_ENV=dev
APP_CATALOG=mapping_demo_dev

APP_CRM_LANDING_PATH=/Volumes/mapping_demo_dev/bronze/crm_landing_ext/
APP_ERP_LANDING_PATH=/Volumes/mapping_demo_dev/bronze/erp_landing_ext/
APP_REFERENCE_LANDING_PATH=/Volumes/mapping_demo_dev/bronze/reference_landing_ext/
APP_COMPLIANCE_LANDING_PATH=/Volumes/mapping_demo_dev/bronze/compliance_landing_ext/

APP_CHECKPOINT_PATH=/Volumes/mapping_demo_dev/bronze/checkpoints_ext/
APP_ARCHIVE_PATH=/Volumes/mapping_demo_dev/bronze/archive_ext/
APP_REJECTED_PATH=/Volumes/mapping_demo_dev/bronze/rejected_ext/
```

---

## Databricks Workflow Execution

### 1. Authentication

Authenticate your local CLI with the target Databricks workspace:

```bash
databricks auth login --host "<databricks_workspace_url>"
```

### 2. Validate and Deploy Bundle

```bash
# Validate the asset bundle syntax and targets
databricks bundle validate --target dev

# Deploy the bundle configurations and files to the workspace
databricks bundle deploy --target dev
```

### 3. Upload Sample Data

Before triggering the pipeline, seed the Unity Catalog External Volumes with the provided sample data:

```bash
# CRM & ERP Source Data
databricks fs cp sample_data/crm_customers.csv dbfs:/Volumes/mapping_demo_dev/bronze/crm_landing_ext/crm_customers.csv --overwrite
databricks fs cp sample_data/erp_clients.csv dbfs:/Volumes/mapping_demo_dev/bronze/erp_landing_ext/erp_clients.csv --overwrite

# Reference Data
databricks fs cp sample_data/semantic_concepts.csv dbfs:/Volumes/mapping_demo_dev/bronze/reference_landing_ext/semantic_concepts.csv --overwrite

# Compliance Configs
databricks fs cp sample_data/consent_preferences.csv dbfs:/Volumes/mapping_demo_dev/bronze/compliance_landing_ext/consent_preferences.csv --overwrite
databricks fs cp sample_data/deletion_requests.csv dbfs:/Volumes/mapping_demo_dev/bronze/compliance_landing_ext/deletion_requests.csv --overwrite
databricks fs cp sample_data/data_retention_policy.csv dbfs:/Volumes/mapping_demo_dev/bronze/compliance_landing_ext/data_retention_policy.csv --overwrite
```

Verify that the files were successfully uploaded:

```bash
databricks fs ls dbfs:/Volumes/mapping_demo_dev/bronze/crm_landing_ext/
```

### 4. Run the Pipeline

Trigger the pipeline workflow via the Databricks Asset Bundle:

```bash
databricks bundle run gdpr_mapping_pipeline --target dev
```

---

## Pipeline Stages Breakdown

The `databricks.yml` file configures the `gdpr_mapping_pipeline_dev` job using an ephemeral single-node job cluster to control runtime costs.

| Stage | Description |
|---|---|
| **`00_external_volume_smoke_test`** | Validates access to Unity Catalog external volumes. Attempts to list paths, write a test file, read it back, and delete it. If this step fails, the root cause is structural/permissions (Azure RBAC, Storage Credentials, Access Connector) rather than application logic. |
| **`01_ingest`** | Loads landing CSV files into Raw Bronze Delta Tables using overwrite mode (optimized for development iterations). Adds technical metadata columns: `_source_system`, `_source_file`, `_ingested_at`, `_batch_id`, and `_record_hash` (a deterministic hash used to identify row-level changes). |
| **`02_profile_sources`** | Profiles columns from Bronze CRM and ERP data, outputting quality metrics (e.g., `null_rate`, `distinct_rate`, `inferred_type`, length bounds) into `metadata.source_column_profile`. |
| **`03_semantic_mapping`** | Compares source column profiles against a semantic dictionary (`concept_id`, `pii_category`, etc.). Evaluates structures to output mapping decisions (`AUTO_ACCEPT`, `REVIEW_REQUIRED`, `LOW_CONFIDENCE`) into metadata management tables. |
| **`04_standardize_and_mask`** | Standardizes structural attributes (names, emails, phones) and creates pseudonymous keys (`customer_pseudo_id`). Production salts/peppers must be injected from Key Vault via secret scopes. A fallback local salt is used strictly for deterministic dev testing. |
| **`05_apply_compliance_controls`** | Evaluates compliance regulations (consents, active deletion requests, expiration policies) to output flags like `can_process_for_analytics` or `can_process_for_marketing`. `NO_MARKETING_CONSENT` allows entity resolution; `NO_ANALYTICS_CONSENT` blocks it. `DELETION_REQUESTED` and `RETENTION_EXPIRED` act as absolute global blockers. |

---

## The `mapping_engine` Package

Core Python modules containing backend logic decoupled from job wrappers:

| Module | Description |
|---|---|
| `config.py` | Centralizes workspace paths and structural settings. |
| `ingest.py` | Core logic for `BronzeIngestSpec` and initial schema mapping. |
| `profiling.py` | Generates column-level statistical data. |
| `semantics.py` | Contains evaluation heuristics for semantic classifications. |
| `standardization.py` & `masking.py` | Dual-implementation helper methods — Pure Python scalars for local unit tests and Spark Column expressions for cluster-wide processing. |
| `compliance.py` | Pure Python domain execution for rule auditing. |
| `compliance_controls.py` | Spark DataFrame implementations executing production compliance filtering joins. |

---

## Post-Run SQL Validation

Validation queries reside under `sql/validation/`. These are executed manually via Databricks SQL Warehouses or Notebooks to verify output states.

```sql
-- Example: Verify object instantiation
SHOW TABLES IN mapping_demo_dev.bronze;

-- Example: Check ingestion consistency
SELECT COUNT(*) FROM mapping_demo_dev.bronze.crm_customers_raw;

-- Example: Inspect compliance results
SELECT * FROM mapping_demo_dev.compliance.processing_eligibility
ORDER BY source_system, source_record_id;
```

---

## Expected Output Tables Matrix

| Schema | Table Name | Description |
|---|---|---|
| **Bronze** | `crm_customers_raw`, `erp_clients_raw`, `semantic_concepts_raw`, `consent_preferences_raw`, `deletion_requests_raw`, `data_retention_policy_raw` | Raw ingestion points containing added technical metadata. |
| **Metadata** | `source_column_profile`, `semantic_mapping_candidates`, `mapping_decisions` | Column diagnostics and candidate AI/deterministic semantic evaluations. |
| **Silver** | `crm_customers_standardized`, `erp_clients_standardized`, `customer_identity_candidates` | Normalized and pseudonymized core entities. |
| **Compliance** | `consent_status`, `deletion_requests`, `retention_policy`, `processing_eligibility`, `compliance_events` | Granular eligibility evaluation matrices and event logs. |

---

## Current Architecture Limitations

This version is a dev/demo implementation with known limits by design:

- Ingest stages use full overwrite modes instead of streaming Auto Loader or incremental `MERGE` statements.
- Jobs load code directly via a temporary `sys.path` injection of the workspace `/src` directory rather than utilizing production-compiled wheel deployments (which remains the targeted future packaging approach).
- Semantic mapping rules output metadata evaluations but do not dynamically rewrite Silver generation steps yet.

---

## CI/CD Status

The current GitHub Actions skeleton supports automated PR checks on push/pull requests:

- **Linting & Unit Tests:** Runs `ruff check .` and `pytest` using an editable install pipeline.
- **Bundle Verification:** Installs the Databricks CLI and triggers `databricks bundle validate`.

Continuous Deployment (automated bundle application) via OIDC / Workload Identity Federation with manual approval steps is planned for future hardening.

---

## Teardown and Cost Control Notes

Workloads are deployed onto an ephemeral job cluster that spins down immediately following execution to limit compute leakage.

To clear down development assets completely, follow this order:

1. Purge runtime tables via Databricks SQL or notebooks (as they are application-level artifacts not tracked by infrastructure engines).
2. Destroy Asset Bundle resources via the Databricks CLI.
3. Tear down infrastructure layers sequentially using the deployment pipelines found in Repo 2 (`data-mapping-infra`) and Repo 1 (`data-mapping-bootstrap`).