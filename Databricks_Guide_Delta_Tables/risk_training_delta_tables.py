# Databricks notebook source
# DBTITLE 1,Step 1 - Create your schema and volume
catalog = "workspace" # <- replace with your catalog name from Step 0
schema = "risk_training"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_files")
print(f"Volume path: /Volumes/{catalog}/{schema}/raw_files")

# COMMAND ----------

# DBTITLE 1,Step 3 - First Delta table from CSV (15 minutes)
catalog = "workspace" # same value as in Step 1
schema = "risk_training"
df = (spark.read
.option("header", True)
.option("inferSchema", True)
.csv(f"/Volumes/{catalog}/{schema}/raw_files/01_loans_2025.csv"))
df.printSchema() # check inferred types
df.show(5)
df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.loans_raw")

# COMMAND ----------

# DBTITLE 1,Step 4 - Excel file and appending a new year (15 minutes)
# MAGIC %pip install xlrd
# MAGIC
# MAGIC import pandas as pd
# MAGIC pdf = pd.read_excel(f"/Volumes/{catalog}/{schema}/raw_files/02_loans_2026.xls",
# MAGIC engine="xlrd")
# MAGIC df_new = spark.createDataFrame(pdf)
# MAGIC df_new.write.mode("append").saveAsTable(f"{catalog}.{schema}.loans_raw")
# MAGIC spark.table(f"{catalog}.{schema}.loans_raw").count() # expect 18

# COMMAND ----------

# DBTITLE 1,Step 5 - Transform and aggregate: the results table (20 minutes)
# Compute default rates by rating class and by segment, and store them in a new Delta table. This is the analyst heart of the workflow: SQL you already know, persisted as a governed table.

result = spark.sql(f"""
SELECT
rating,
count(*) AS n_loans,
sum(loan_amount) AS exposure,
avg(CASE WHEN status = 'default' THEN 1.0 ELSE 0.0 END) AS default_rate
FROM {catalog}.{schema}.loans_raw
GROUP BY rating
ORDER BY rating
""")
result.show()
result.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.default_rates_by_rating")

# COMMAND ----------

# DBTITLE 1,Comment table using SQL
# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.risk_training.default_rates_by_rating IS
# MAGIC 'Default rate and exposure by rating class, aggregated from loans_raw. Training
# MAGIC data, synthetic.';

# COMMAND ----------

# DBTITLE 1,Step - Apply corrections to existing observations
# Load corrections.csv into a Spark DataFrame
corr = (spark.read
.option("header", True)
.option("inferSchema", True)
.csv(f"/Volumes/{catalog}/{schema}/raw_files/03_corrections.csv"))
corr.createOrReplaceTempView("corrections")

# Merge corrections into the raw data
spark.sql(f"""
MERGE INTO {catalog}.{schema}.loans_raw t
USING corrections c
ON t.loan_id = c.loan_id AND t.obs_year = c.obs_year
WHEN MATCHED THEN UPDATE SET t.status = c.new_status
""")

# COMMAND ----------

# DBTITLE 1,Step 7a - Version control with time travel
# Every write, append, and merge created a version. Inspect the history, query an old version, and restore if needed.
spark.sql(f"DESCRIBE HISTORY {catalog}.{schema}.loans_raw").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 7b - Version control with time travel
# MAGIC %sql
# MAGIC -- what did the table look like before the merge?
# MAGIC SELECT * FROM workspace.risk_training.loans_raw VERSION AS OF 2 WHERE
# MAGIC loan_id = 'L004';

# COMMAND ----------

# DBTITLE 1,Step 7c - Version control with time travel
# MAGIC %sql
# MAGIC -- undo the merge entirely (creates yet another version)
# MAGIC RESTORE TABLE workspace.risk_training.loans_raw TO VERSION AS OF 2;

# COMMAND ----------

# DBTITLE 1,Step 9 — Consume in the SQL editor and wrap up
# MAGIC %sql
# MAGIC SELECT * FROM workspace.risk_training.default_rates_by_rating ORDER BY rating;