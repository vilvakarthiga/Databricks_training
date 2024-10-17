# Databricks notebook source
# MAGIC %run /Workspace/Users/vilvakarthigaece@gmail.com/Day1/includes

# COMMAND ----------

df_sales=spark.read.json(f"{input_path}products.json",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df_sales)

# COMMAND ----------



# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("products")
