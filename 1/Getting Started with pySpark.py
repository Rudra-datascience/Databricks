# Databricks notebook source
# DBTITLE 1,Simple Python Code to Print Hello Message
print("Hello")

# COMMAND ----------

data = [(1,'a',20),(2,'b',30)]
#schema = ["Id","Name","Age"]
schema = ["Id", "Name", "Age"]
df = spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df.select("Id","Name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("Id").alias("Emp Id")).display()

# COMMAND ----------

df.withColumnRenamed

# COMMAND ----------

df.withColumn("Current Date",current_date()).display()

# COMMAND ----------

df.withColumn("Current Date", current_date()).display()
