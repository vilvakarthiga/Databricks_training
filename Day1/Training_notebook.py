# Databricks notebook source
data = [(1, "a", 10), (2, "b", 20)]
schema="id int,name string,age int"
df=spark.createDataFrame(data,schema)
df.display()
 

# COMMAND ----------

df1=df.select("id","name").display()

# COMMAND ----------

df.select("id",alias("emp_id"))

# COMMAND ----------

from pyspark.sql.functions import*

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("current_date",current_date()).display()

# COMMAND ----------

df.withColumn("age",current_date()).display()
