# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading NOCS data

# COMMAND ----------

df = spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True)\
    .load("abfss://bronze@olympicsa.dfs.core.windows.net/nocs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping Column

# COMMAND ----------

df = df.drop('country')

# COMMAND ----------

df = df.withColumn('tag',split(col('tag'),'-')[0])

# COMMAND ----------

df.write.format('delta')\
    .mode('append')\
    .option('path','abfss://silver@olympicsa.dfs.core.windows.net/nocs')\
    .save()