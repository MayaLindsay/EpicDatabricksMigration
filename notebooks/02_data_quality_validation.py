# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime, timezone
import uuid

catalog = "epicdatabricksimplementation"
run_id = str(uuid.uuid4())
run_time = datetime.now(timezone.utc)

tables = [
    "patientdim",
    "providerdim",
    "billingaccounts",
    "glp1extract"
]

results = []

def record_result(table, rule, failed_count, severity="CRITICAL"):
    results.append((
        run_id,
        run_time,
        table,
        rule,
        int(failed_count),
        severity,
        "FAIL" if failed_count > 0 else "PASS"
    ))

# 1. Confirm all Bronze tables exist and contain data
for table in tables:
    full_name = f"{catalog}.bronze.{table}"

    if not spark.catalog.tableExists(full_name):
        record_result(table, "table_exists", 1)
        continue

    df = spark.table(full_name)

    record_result(
        table,
        "table_not_empty",
        1 if df.limit(1).count() == 0 else 0
    )

# 2. Validate patient dimension
patient_table = f"{catalog}.bronze.patientdim"

if spark.catalog.tableExists(patient_table):
    df = spark.table(patient_table)

    if "patient_mrn" not in df.columns:
        record_result("patientdim", "patient_mrn_column_exists", 1)
    else:
        valid_mrn = F.trim(F.col("patient_mrn").cast("string"))

        missing = df.filter(
            valid_mrn.isNull() | (valid_mrn == "")
        ).count()

        duplicates = (
            df.filter(valid_mrn.isNotNull() & (valid_mrn != ""))
            .groupBy(valid_mrn.alias("mrn"))
            .count()
            .filter(F.col("count") > 1)
            .agg(F.sum(F.col("count") - 1).alias("excess"))
            .first()["excess"]
        ) or 0

        record_result("patientdim", "missing_patient_mrn", missing)
        record_result("patientdim", "duplicate_patient_mrn", duplicates)

    for column in [
        "glp1_registry_y_n",
        "diabetes_registry_y_n"
    ]:
        if column in df.columns:
            normalized = F.upper(F.trim(F.col(column)))

            invalid = df.filter(
                normalized.isNull() |
                (~normalized.isin("Y", "N"))
            ).count()

            record_result(
                "patientdim",
                f"invalid_{column}",
                invalid,
                "WARNING"
            )
        else:
            record_result(
                "patientdim",
                f"missing_column_{column}",
                1,
                "WARNING"
            )

# 3. Save the audit results
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.governance"
)

audit_columns = [
    "run_id",
    "run_timestamp",
    "table_name",
    "rule_name",
    "failed_count",
    "severity",
    "status"
]

audit_df = spark.createDataFrame(results, audit_columns)

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.governance.data_quality_audit")
)

display(audit_df)

# 4. Stop the pipeline if critical checks fail
critical_failures = [
    r for r in results
    if r[5] == "CRITICAL" and r[6] == "FAIL"
]

if critical_failures:
    raise RuntimeError(
        f"Data quality gate FAILED: "
        f"{len(critical_failures)} critical rules failed. "
        f"Run ID: {run_id}"
    )

print(f"DATA QUALITY GATE PASSED. Run ID: {run_id}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_timestamp,
# MAGIC     table_name,
# MAGIC     rule_name,
# MAGIC     failed_count,
# MAGIC     severity,
# MAGIC     status
# MAGIC FROM epicdatabricksimplementation.governance.data_quality_audit
# MAGIC ORDER BY run_timestamp DESC, table_name, rule_name;