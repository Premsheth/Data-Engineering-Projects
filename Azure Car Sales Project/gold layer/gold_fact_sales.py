# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE FACT TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------

df_silver = spark.sql(" select * from parquet.`abfss://silver@azurecarprojectsg.dfs.core.windows.net/carsales`")
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading all the Dims**

# COMMAND ----------

df_dealer = spark.sql("select * from cars_catalog.gold.dim_dealer")

df_date = spark.sql("select * from cars_catalog.gold.dim_date")

df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")

df_model = spark.sql("select * from cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %md
# MAGIC **Bringing Keys to the Fact table**

# COMMAND ----------

df_fact = df_silver.join(df_dealer,df_silver['Dealer_ID'] == df_dealer['Dealer_ID'],how = 'left')\
                    .join(df_branch,df_silver['Branch_ID'] == df_branch['Branch_ID'],how = 'left')\
                    .join(df_model,df_silver['Model_ID'] == df_model['Model_ID'],how = 'left')\
                    .join(df_date,df_silver['Date_ID'] == df_date['Date_ID'],how = 'left')\
                    .select(df_silver['Revenue'],df_silver['Unit_sold'],df_silver['RevUnit'],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'], \
                        df_date['dim_date_key'],df_model['dim_model_key'])


# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing duplicate**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Define window grouped by your merge key and ordered by a timestamp
windowSpec = Window.partitionBy("dim_branch_key","dim_dealer_key","dim_model_key","dim_date_key").orderBy("dim_date_key")

# Filter to keep only the newest record per key
deduplicated_source_df = df_fact \
    .withColumn("row_num", row_number().over(windowSpec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# COMMAND ----------

deduplicated_source_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Writing Fact table to Data lake

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.factsales'):
    deltatable = DeltaTable.forName(spark, 'cars_catalog.gold.factsales')

    deltatable.alias('trg').merge(deduplicated_source_df.alias('src'),'trg.dim_date_key = src.dim_date_key \
                                                        and trg.dim_branch_key = src.dim_branch_key \
                                                        and trg.dim_dealer_key = src.dim_dealer_key \
                                                        and trg.dim_model_key = src.dim_model_key')\
                                                        .whenMatchedUpdateAll()\
                                                        .whenNotMatchedInsertAll()\
                                                        .execute()

else:
    deduplicated_source_df.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@azurecarprojectsg.dfs.core.windows.net/factsales')\
        .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales
# MAGIC

# COMMAND ----------

