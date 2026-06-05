# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

dbutils.widgets.text("source_container","")
dbutils.widgets.text("sink_folder","")
dbutils.widgets.text("folder","")

# COMMAND ----------

source_container = dbutils.widgets.get("source_container")
sink_folder = dbutils.widgets.get("sink_folder")
folder = dbutils.widgets.get("folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameterizing Code

# COMMAND ----------

df = spark.read.format('parquet').load(f"abfss://{source_container}@olympicsa.dfs.core.windows.net/{folder}")

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"abfss://{sink_folder}@olympicsa.dfs.core.windows.net/{folder}") \
    .save()

# COMMAND ----------

