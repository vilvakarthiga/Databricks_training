# Databricks notebook source
spark sql
pyspark df

# COMMAND ----------

df=spark.read.csv("/Volumes/databrickstraining_vilva/default/raw_files/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df_customer=spark.read.json("/Volumes/databrickstraining_vilva/default/raw_files/customers.json")

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_customer.write.saveAsTable("customer")


# COMMAND ----------

df.write.saveAsTable("Sales")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("Sales")
