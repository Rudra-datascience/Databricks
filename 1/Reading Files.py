# Databricks notebook source
df=spark.read.csv("/Volumes/rudra_databricks/default/raw_data/sales.csv")

# COMMAND ----------



# COMMAND ----------

df_sales = spark.read.csv("/Volumes/rudra_databricks/default/raw_data/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.saveAsTable("sales")

# COMMAND ----------

df_

# COMMAND ----------

def add_ingestion(df):
    df1 = 
