# Databricks notebook source
# MAGIC %sql
# MAGIC create table customers as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/databrickstraining_vilva/default/raw_files/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table products as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/databrickstraining_vilva/default/raw_files/products.json`

# COMMAND ----------


