# Databricks notebook source
# MAGIC %run /Workspace/Users/rudradip.chakrabarti@gmail.com/includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.saveAsTable("sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, count(*)
# MAGIC from sales
# MAGIC group by customer_id
# MAGIC order by customer_id

# COMMAND ----------

df_sales.filter("customer_id=2").display()

# COMMAND ----------

df_sales.where("customer_id">2 or order_id ==20").display()

# COMMAND ----------

df_customer.sort("customer_city",ascending=True).display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales

# COMMAND ----------


