# Databricks notebook source
volume_path = "/Volumes/epicdatabricksimplementation/epic_raw/source_files/"

files = dbutils.fs.ls(volume_path)

display(files)

# COMMAND ----------

df_glp1 = (
    spark.read
    .format("excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        "/Volumes/epicdatabricksimplementation/epic_raw/source_files/glp1extract_v1.xlsx"
    )
)

display(df_glp1.limit(10))

# COMMAND ----------

df_glp1 = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "/Volumes/epicdatabricksimplementation/epic_raw/source_files/glp1extract_v1.xlsx"
    )
)

display(df_glp1.limit(10))

# COMMAND ----------

source_path = "/Volumes/epicdatabricksimplementation/epic_raw/source_files/"

file_names = [
    "patientdim_v1",
    "providerdim_v1",
    "billingaccounts_v1",
    "glp1extract_v1"
]

for name in file_names:

    df = (
        spark.read
        .option("header", True)
        .csv(f"{source_path}{name}.csv")
    )

    print(f"File: {name}.csv")
    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")
    print(f"Column Names: {df.columns}")
    print("-" * 50)

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC import pandas as pd
# MAGIC
# MAGIC source_path = "/Volumes/epicdatabricksimplementation/epic_raw/source_files/"
# MAGIC
# MAGIC file_names = [
# MAGIC     "patientdim_v1",
# MAGIC     "providerdim_v1",
# MAGIC     "billingaccounts_v1",
# MAGIC     "glp1extract_v1"
# MAGIC ]
# MAGIC
# MAGIC for name in file_names:
# MAGIC
# MAGIC     df = pd.read_excel(
# MAGIC         f"{source_path}{name}.xlsx",
# MAGIC         engine="openpyxl"
# MAGIC     )
# MAGIC
# MAGIC     print(f"File: {name}.xlsx")
# MAGIC     print(f"Rows: {len(df)}")
# MAGIC     print(f"Columns: {len(df.columns)}")
# MAGIC     print(f"Column Names: {list(df.columns)}")
# MAGIC     print("-" * 50)

# COMMAND ----------

import re

def clean_column_name(column_name):
    # Convert to lowercase
    column_name = column_name.lower().strip()

    # Replace spaces and special characters with underscores
    column_name = re.sub(r"[^a-z0-9_]", "_", column_name)

    # Remove duplicate underscores
    column_name = re.sub(r"_+", "_", column_name)

    # Remove leading/trailing underscores
    return column_name.strip("_")

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC import pandas as pd
# MAGIC import re
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
# MAGIC def clean_column_name(name):
# MAGIC     name = name.lower().strip()
# MAGIC     name = re.sub(r"[^a-z0-9_]", "_", name)
# MAGIC     name = re.sub(r"_+", "_", name)
# MAGIC     return name.strip("_")
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
# MAGIC         ).fillna("")
# MAGIC
# MAGIC         df = spark.createDataFrame(pdf.astype(str))
# MAGIC
# MAGIC     else:
# MAGIC         raise FileNotFoundError(
# MAGIC             f"Source file not found for {name}"
# MAGIC         )
# MAGIC
# MAGIC     # Standardize column names
# MAGIC     cleaned_columns = [
# MAGIC         clean_column_name(c) for c in df.columns
# MAGIC     ]
# MAGIC
# MAGIC     # Stop if normalization creates duplicate names
# MAGIC     if len(cleaned_columns) != len(set(cleaned_columns)):
# MAGIC         raise ValueError(
# MAGIC             f"Duplicate column names after cleaning: {name}"
# MAGIC         )
# MAGIC
# MAGIC     df = df.toDF(*cleaned_columns)
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
# MAGIC         .format("delta")
# MAGIC         .mode("overwrite")
# MAGIC         .saveAsTable(target_table)
# MAGIC     )
# MAGIC
# MAGIC     print(
# MAGIC         f"SUCCESS: {target_table} "
# MAGIC         f"loaded with {df.count()} rows"
# MAGIC     )

# COMMAND ----------

df = spark.table(
    "epicdatabricksimplementation.bronze.patientdim"
)

print(df.columns)
display(df.limit(10))

# COMMAND ----------

catalog = "epicdatabricksimplementation"

tables = [
    "patientdim",
    "providerdim",
    "billingaccounts",
    "glp1extract"
]

for table in tables:
    full_name = f"{catalog}.bronze.{table}"

    print(f"\nViewing: {full_name}")

    df = spark.table(full_name)

    print("Total Rows:", df.count())
    print("Total Columns:", len(df.columns))
    print("Column Names:", df.columns)

    display(df.limit(10))