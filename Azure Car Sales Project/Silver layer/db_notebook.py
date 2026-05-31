# Databricks notebook source
# MAGIC %md
# MAGIC # Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS cars_catalog;
# MAGIC USE CATALOG cars_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists cars_catalog.silver;
# MAGIC create schema if not exists cars_catalog.gold;