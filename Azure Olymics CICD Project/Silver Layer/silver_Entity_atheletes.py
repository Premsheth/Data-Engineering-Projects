# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
    .load("abfss://bronze@olympicsa.dfs.core.windows.net/athletes")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ***Handling NULL values in BirthPlace and Residence_Country column***

# COMMAND ----------

df = df.fillna({"birth_place":"xyz","birth_country":"abc","residence_place":"Unknown","residence_country":"aaa"})

# COMMAND ----------

# MAGIC %md
# MAGIC ***Filter data with below conditions***
# MAGIC 1. Filter data with name 'GALSTYAN Slavik' and 'HARUTYUNYAN Arsen' and They playing currently

# COMMAND ----------

df_filter = df.filter((col("current") == True) & (col('name').isin('GALSTYAN Slavik','HARUTYUNYAN Arsen','SEHEN Sajjad')))
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ***Cast weight and height column in Float type and sort data with ascending weight and descending height column and filter weight > 0***

# COMMAND ----------

df = df.withColumn("weight",col("weight").cast(FloatType()))\
    .withColumn("height",col("height").cast(FloatType()))

# COMMAND ----------

df_sorted = df.sort('height','weight',ascending=[0,1]).filter(col('weight') > 0)

# COMMAND ----------

df_sorted = df_sorted.withColumn('nationality',regexp_replace(col('nationality'),'United States','US'))
df_sorted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ***Find duplicate values in Code column***

# COMMAND ----------

df.groupBy('code').agg(count('code').alias('total_count')).filter(col('total_count') > 1).display()

# COMMAND ----------

df_sorted = df_sorted.withColumnRenamed('code','athelete_id')
df_sorted.display()

# COMMAND ----------

df_sorted = df_sorted.withColumn('occupation',split(col('occupation'),','))

# COMMAND ----------

df_sorted.columns

# COMMAND ----------

df_final = df_sorted.select('athelete_id',
 'current',
 'name',
 'name_short',
 'name_tv',
 'gender',
 'function',
 'country_code',
 'country',
 'country_long',
 'nationality',
 'nationality_long',
 'nationality_code',
 'height',
 'weight')

# COMMAND ----------

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ***Cummulative sum of weight based on natinality***

# COMMAND ----------

df_final.withColumn('cum_weight',sum('weight').over(Window.partitionBy('nationality').orderBy('height').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))

# COMMAND ----------

df_final.write.format('delta')\
    .mode('append')\
    .option('path','abfss://silver@olympicsa.dfs.core.windows.net/athletes')\
    .saveAsTable('`olympics-catalog`.silver.athletes')

# COMMAND ----------

olympics-catalog