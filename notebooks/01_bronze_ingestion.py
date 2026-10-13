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