# Databricks notebook source
catalog = "epicdatabricksimplementation"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.bronze
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.silver
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.gold
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.governance
""")

print("Schemas verified successfully.")

# COMMAND ----------

display(
    spark.sql(
        "SHOW TABLES IN epicdatabricksimplementation.bronze"
    )
)

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC import pandas as pd
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC catalog = "epicdatabricksimplementation"
# MAGIC source_path = f"/Volumes/{catalog}/epic_raw/source_files/"
# MAGIC
# MAGIC datasets = [
# MAGIC     "patientdim_v1",
# MAGIC     "providerdim_v1",
# MAGIC     "billingaccounts_v1",
# MAGIC     "glp1extract_v1"
# MAGIC ]
# MAGIC
# MAGIC spark.sql(
# MAGIC     f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze"
# MAGIC )
# MAGIC
# MAGIC available_files = {
# MAGIC     file.name for file in dbutils.fs.ls(source_path)
# MAGIC }
# MAGIC
# MAGIC for name in datasets:
# MAGIC
# MAGIC     csv_file = f"{name}.csv"
# MAGIC     excel_file = f"{name}.xlsx"
# MAGIC
# MAGIC     if csv_file in available_files:
# MAGIC         source_file = csv_file
# MAGIC
# MAGIC         df = (
# MAGIC             spark.read
# MAGIC             .option("header", True)
# MAGIC             .option("inferSchema", False)
# MAGIC             .csv(f"{source_path}{source_file}")
# MAGIC         )
# MAGIC
# MAGIC     elif excel_file in available_files:
# MAGIC         source_file = excel_file
# MAGIC
# MAGIC         pdf = pd.read_excel(
# MAGIC             f"{source_path}{source_file}",
# MAGIC             engine="openpyxl",
# MAGIC             dtype=str
# MAGIC         )
# MAGIC
# MAGIC         # Handle empty Excel cells
# MAGIC         pdf = pdf.fillna("")
# MAGIC
# MAGIC         # Convert pandas DataFrame to Spark
# MAGIC         df = spark.createDataFrame(pdf.astype(str))
# MAGIC
# MAGIC     else:
# MAGIC         raise FileNotFoundError(
# MAGIC             f"No CSV or Excel source found for {name}"
# MAGIC         )
# MAGIC
# MAGIC     # Add ingestion metadata
# MAGIC     df = (
# MAGIC         df.withColumn(
# MAGIC             "ingestion_timestamp",
# MAGIC             F.current_timestamp()
# MAGIC         )
# MAGIC         .withColumn(
# MAGIC             "source_file",
# MAGIC             F.lit(source_file)
# MAGIC         )
# MAGIC     )
# MAGIC
# MAGIC     table_name = name.replace("_v1", "")
# MAGIC     target_table = f"{catalog}.bronze.{table_name}"
# MAGIC
# MAGIC     (
# MAGIC         df.write
# MAGIC     .format("delta")
# MAGIC     .mode("overwrite")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .saveAsTable(target_table)
# MAGIC     )
# MAGIC
# MAGIC     print(
# MAGIC         f"SUCCESS: {target_table} "
# MAGIC         f"loaded with {df.count()} rows"
# MAGIC     )

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "epicdatabricksimplementation"

tables = [
    "patientdim",
    "providerdim",
    "billingaccounts",
    "glp1extract"
]

profile_results = []

for table in tables:
    df = spark.table(f"{catalog}.bronze.{table}")
    row_count = df.count()

    for column in df.columns:
        c = F.col(f"`{column}`")

        missing_count = df.filter(
            c.isNull() | (F.trim(c.cast("string")) == "")
        ).count()

        profile_results.append((
            table,
            column,
            row_count,
            missing_count,
            round(
                missing_count / row_count * 100, 2
            ) if row_count else 0.0
        ))

profile_df = spark.createDataFrame(
    profile_results,
    ["table_name", "column_name", "row_count",
     "missing_count", "missing_pct"]
)

display(profile_df)

# COMMAND ----------

from pyspark.sql import functions as F

patient_df = spark.table(
    "epicdatabricksimplementation.bronze.patientdim"
)

# Check required patient identifiers
missing_mrn = patient_df.filter(
    F.col("patient_mrn").isNull() |
    (F.trim(F.col("patient_mrn")) == "")
).count()

# Check duplicate patient identifiers
duplicate_mrn = (
    patient_df
    .filter(
        F.col("patient_mrn").isNotNull() &
        (F.trim(F.col("patient_mrn")) != "")
    )
    .groupBy("patient_mrn")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Missing Patient MRNs:", missing_mrn)
print("Duplicate Patient MRN values:", duplicate_mrn)

# Inspect GLP-1 and diabetes registry values
for column in [
    "glp1_registry_y_n",
    "diabetes_registry_y_n"
]:
    if column in patient_df.columns:
        print(f"\nRegistry values: {column}")
        display(
            patient_df.groupBy(column)
            .count()
            .orderBy(column)
        )

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("""
CREATE SCHEMA IF NOT EXISTS
epicdatabricksimplementation.governance
""")

audit_df = (
    profile_df
    .withColumn("run_timestamp", F.current_timestamp())
)

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "epicdatabricksimplementation.governance."
        "data_quality_profile"
    )
)

print("Data quality results saved successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM epicdatabricksimplementation.governance.data_quality_profile
# MAGIC ORDER BY run_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE
# MAGIC epicdatabricksimplementation.bronze.patientdim
# MAGIC IS 'Bronze patient dimension containing raw
# MAGIC Epic-inspired patient and registry information';
# MAGIC
# MAGIC COMMENT ON TABLE
# MAGIC epicdatabricksimplementation.bronze.glp1extract
# MAGIC IS 'Bronze GLP-1 treatment extract for
# MAGIC population health analytics';
# MAGIC
# MAGIC COMMENT ON TABLE
# MAGIC epicdatabricksimplementation.bronze.billingaccounts
# MAGIC IS 'Bronze billing account data for
# MAGIC healthcare financial analytics';
# MAGIC
# MAGIC COMMENT ON TABLE
# MAGIC epicdatabricksimplementation.bronze.providerdim
# MAGIC IS 'Bronze provider reference data';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_patients,
# MAGIC     COUNT(DISTINCT patient_mrn) AS unique_patients
# MAGIC FROM epicdatabricksimplementation.bronze.patientdim;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE
# MAGIC epicdatabricksimplementation.bronze.patientdim;
# MAGIC