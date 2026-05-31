# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('parquet')\
        .option('inferSchema',True)\
        .load('abfss://bronze@azurecarprojectsg.dfs.core.windows.net/rawdata/')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split Model_ID column through delimeter '- ' in Model_Category and Model_ID

# COMMAND ----------

df_new = df.withColumn('Model_Category',split(col('Model_ID'), '-').getItem(0))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Unit_Sold columnn datatyupe to string

# COMMAND ----------

df_new = df_new.withColumn('Unit_sold',col('Unit_Sold').cast(StringType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Revenue per unit using Revenue/unit_sold

# COMMAND ----------

df_new = df_new.withColumn('RevUnit',col('Revenue')/col('Unit_sold'))
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC

# COMMAND ----------

# MAGIC %md
# MAGIC ## How many units sold in each Branch Every year?

# COMMAND ----------

df_new.groupBy('Year','BranchName').agg(sum('Unit_sold').alias('Total_Unit')).sort('Year','Total_Unit',ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df_new.write.format('parquet')\
    .mode('overwrite')\
    .option('path','abfss://silver@azurecarprojectsg.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@azurecarprojectsg.dfs.core.windows.net/carsales`

# COMMAND ----------

