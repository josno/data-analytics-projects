# Databricks notebook source
# MAGIC %md
# MAGIC ## Create 'Customer Curated' Temp Table by Joining Accelerator_Trusted & Customer Trusted
# MAGIC - Let's us see only customers who generated accelerometer data
# MAGIC

# COMMAND ----------

spark.catalog.dropTempView("customer_curated")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW customer_curated AS
# MAGIC SELECT 
# MAGIC   DISTINCT c.*
# MAGIC FROM stedi.customer_trusted as c
# MAGIC INNER JOIN stedi.accelerometer_trusted as a
# MAGIC ON c.email = a.user;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customer_curated
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM customer_curated;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write New Table to Curated (Gold) Zone

# COMMAND ----------

cust_curated_path = "mnt/stedi/customer/curated/"

spark.table("customer_curated").write.mode("overwrite").format("delta").save(cust_curated_path)
print(f"Customer curated data written to {cust_curated_path}")


# COMMAND ----------

cust_curated_df = spark.read.format('delta').load('/mnt/stedi/customer/curated/')
display(f'Trusted path total customers: {cust_curated_df.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Curated Customer Table in Catalog for Queries

# COMMAND ----------


# Save as Delta and register in catalog using python
cust_curated_df.write.mode("overwrite").format("delta").saveAsTable("stedi.customer_curated")