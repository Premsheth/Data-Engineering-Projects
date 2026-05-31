# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flg = dbutils.widgets.get('incremental_flag')
print(incremental_flg)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimensions models

# COMMAND ----------

df_src = spark.sql('''
select distinct(Dealer_ID) as Dealer_ID,DealerName from parquet.`abfss://silver@azurecarprojectsg.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model sink Initial and Incremental (Just Bring Schema If table not exist)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
        df_sink = spark.sql('''
                        select dim_dealer_key,Dealer_ID,DealerName
                        from cars_catalog.gold.dim_dealer
                        ''')
else:
    df_sink = spark.sql('''
                        select 1 as dim_dealer_key,Dealer_ID,DealerName 
                        from parquet.`abfss://silver@azurecarprojectsg.dfs.core.windows.net/carsales`
                        where 1 = 0
                        ''')


# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering New Records and Old records

# COMMAND ----------

df_filter = df_src.join(df_sink,df_src['Dealer_ID'] == df_sink['Dealer_ID'],'left')\
    .select(df_src['Dealer_ID'],df_src['DealerName'],df_sink['dim_dealer_key'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create first two different dataframe with Null and not null dim_model_key

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['dim_dealer_key'].isNotNull())

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter['dim_dealer_key'].isNull()).select(df_filter['Dealer_ID'],df_filter['DealerName'])

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch Max surrogate key from exisating column**

# COMMAND ----------

if (incremental_flg == '0'):
    max_value = 1
else:
    max_value_df = spark.sql('''
                          select max(dim_dealer_key) from cars_catalog.gold.dim_dealer
                          ''')
    max_value = max_value_df.collect()[0][0] + 1



# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrogate key Column and add the max ssurrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key',max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final Df - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD TYPE - 1 (UPSERT = Update + Insert)

# COMMAND ----------

#incremental_load
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@azurecarprojectsg.dfs.core.windows.net/dim_dealer')
    delta_tbl.alias('trg')\
        .merge(df_final.alias('src'),'trg.dim_dealer_key = src.dim_dealer_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# Initial Run
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@azurecarprojectsg.dfs.core.windows.net/dim_dealer')\
        .saveAsTable('cars_catalog.gold.dim_dealer')



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer