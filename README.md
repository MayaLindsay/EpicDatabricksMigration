# Epic Healthcare Data Engineering — Databricks Lakehouse

## Project Overview

This project demonstrates the implementation of a healthcare data engineering pipeline using Databricks, Python, PySpark, SQL, Delta Lake, and Unity Catalog. The solution integrates synthetic Epic-inspired clinical and financial datasets to support GLP-1 population health management, Revenue Cycle Management, and Accountable Care Organization (ACO) analytics.

The pipeline follows the Medallion Architecture (Bronze, Silver, and Gold), with the goal of transforming raw healthcare data into validated, analytics-ready datasets for Power BI reporting.

## Source Data

Four healthcare datasets have been uploaded to a Databricks Unity Catalog Managed Volume.

| Source File | Description |
|---|---|
| `patientdim_v1.xlsx` | Patient demographic and reference data |
| `providerdim_v1.xlsx` | Provider demographic and reference data |
| `billingaccounts_v1.xlsx` | Patient billing and financial account information |
| `glp1extract_v1.xlsx` | GLP-1 treatment and clinical data |

**Source location:**

`/Volumes/epicdatabricksimplementation/epic_raw/source_files/`

## Source Data Validation

Python notebooks were developed to verify source file accessibility and inspect dataset structures before ingestion into Delta Lake.

Validation activities include:

- Confirming file availability within the Unity Catalog Volume.
- Reading Excel workbooks using Python and pandas.
- Reviewing record counts and column counts.
- Inspecting source column names and sample records.
- Identifying formatting and data type issues.

**Current status:** Source file verification has been completed. The data preview currently displays delimited columns
