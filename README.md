# chalmers_demo

Databricks Asset Bundle demo for a simple SAP-style pipeline with DLT bronze/silver/gold layers plus an incremental data generator.

## What is inside
- `demo/databricks.yml` defines the bundle and a `lab` target with catalog/schema variables.
- `demo/resources/chalmers_demo_dlt_python.yml` and `demo/resources/chalmers_demo_dlt_sql.yml` define two DLT pipelines (Python and SQL).
- `demo/src/demo-py/transformations` contains the Python DLT tables and a gold materialized view.
- `demo/src/demo-sql/transformations` contains the SQL DLT tables and a gold materialized view.
- `demo/resources/generator.yml` defines a job that runs the incremental data generator notebook.
- `demo/src/incremental-generator/Incremental_data_generator.py` writes CSVs to a Databricks Volume under `/Volumes/chalmers_demo/demo_data/sap_data`.
- `.github/workflows/ci.yml` and `.github/workflows/cd.yml` validate and deploy the bundle with GitHub Actions.

## Pipeline overview
- Bronze: Auto Loader reads customer, material, and sales CSVs from the volume.
- Silver: Cleans strings, fixes types, and applies expectations/constraints.
- Gold: Joins sales with customer/material and computes summary metrics.

The Python and SQL pipelines are parallel implementations of the same logic.

## Prerequisites
- Databricks workspace with access to the Databricks CLI.
- A Unity Catalog catalog and schema (defaults are set in `demo/databricks.yml`).
- A volume at `/Volumes/chalmers_demo/demo_data/sap_data` (created by you or via the generator).

## Local usage
Validate the bundle:
```bash
cd demo
databricks bundle validate -p lab -t lab
```

Deploy the bundle:
```bash
cd demo
databricks bundle deploy -p lab -t lab --force
```

Run the incremental data generator job (after deploy):
```bash
cd demo
databricks bundle run incremental_data -p lab -t lab
```

## CI/CD
- CI runs `databricks bundle validate` on pull requests.
- CD runs `validate` and `deploy` when a PR is merged to `main`.

Both workflows expect `DATABRICKS_HOST` and `DATABRICKS_TOKEN` to be provided via GitHub Actions vars/secrets (`DAB_DEPLOY`).
