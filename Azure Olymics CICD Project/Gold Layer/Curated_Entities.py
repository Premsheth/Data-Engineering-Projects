# Databricks notebook source
# MAGIC %md
# MAGIC # Delta live table - Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coaches DLT Pipeline

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectations for Data Quality

# COMMAND ----------

expec_coaches = {
                    "rule1":"code is not null",
                    "rule2":"current is True",
                }


# COMMAND ----------

expec_nocs = {
                    "rule2":"current is not null",
                }


# COMMAND ----------

expec_coaches = {
                    "rule1":"event is not null",
                }

# COMMAND ----------

@dlt.table

def source_coaches():
    df = spark.readStream.table("`olympics-catalog`.silver.coaches")
    return df

# COMMAND ----------

@dlt.view


def view_coaches():
    df = spark.readStream.table("LIVE.source_coaches")
    df = df.fillna('Unknown')
    return df

# COMMAND ----------

@dlt.table
@dlt.expect_all(expec_coaches)
def coaches():
    df = spark.readStream.table("LIVE.view_coaches")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### NOCS DLT Pipeline

# COMMAND ----------

@dlt.view

def source_nocs():
    df = spark.readStream.table("`olympics-catalog`.silver.nocs")
    return df

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(expec_nocs)
def nocs():
    df = spark.readStream.table("LIVE.source_nocs")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Events DLT Pipeline

# COMMAND ----------

# DBTITLE 1,Cell 11
@dlt.view

def source_events_view():
    df = spark.readStream.table("`olympics-catalog`.silver.events")
    return df

@dlt.table
@dlt.expect_all(expec_events)
def events():
    df = spark.readStream.table("LIVE.source_events_view")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC - Apply changes (DLT)

# COMMAND ----------

@dlt.view

def source_athletes():
    df = spark.readStream.table('`olympics-catalog`.silver.athletes')
    return df


# COMMAND ----------

dlt.create_streaming_table('atheletes')

# COMMAND ----------

dp.create_auto_cdc_flow(
  target = "atheletes",
  source = "source_athletes",
  keys = ["athelete_id"],
  sequence_by = col("height"),
  stored_as_scd_type = 1
)