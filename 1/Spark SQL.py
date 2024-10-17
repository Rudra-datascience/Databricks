# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE table customers as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/rudra_databricks/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

df_customers = spark.table("customers")

# COMMAND ----------

df_customers.display()

# COMMAND ----------



# COMMAND ----------


