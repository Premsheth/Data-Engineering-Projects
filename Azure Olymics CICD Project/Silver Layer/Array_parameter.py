# Databricks notebook source
my_array=[
    {"source_container":"bronze",
     "sink_folder":"silver",
     "folder":"events"},
    {"source_container":"bronze",
     "sink_folder":"silver",
     "folder":"coaches"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "my_output", value = my_array)

# COMMAND ----------

