# Databricks notebook source
# MAGIC %md
# MAGIC ## Set Up Accelerometer Paths to Read and Generate DF

# COMMAND ----------


# Path to customer landing JSON files
acc_landing_path = "/mnt/stedi/accelerometer/landing/"

# Read all JSON files in the folder
acc_landing_df = spark.read.json(acc_landing_path)

# Show schema to verify fields
acc_landing_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Accelerometer Temp Table 

# COMMAND ----------

acc_landing_df.createOrReplaceTempView("accelerometer_landing_temp_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM accelerometer_landing_temp_table
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create accelerometer trusted table filtered against customer trusted

# COMMAND ----------

spark.catalog.dropTempView("accelerometer_trusted")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW accelerometer_trusted AS
# MAGIC SELECT 
# MAGIC   a.*
# MAGIC FROM accelerometer_landing_temp_table as a
# MAGIC INNER JOIN stedi.customer_trusted as c
# MAGIC ON a.user = c.email;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM accelerometer_trusted;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM accelerometer_trusted LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write New Dataframe to Trusted (Silver) Zone

# COMMAND ----------

acc_trusted_path = "/mnt/stedi/accelerometer/trusted/"

spark.table("accelerometer_trusted").write.mode("overwrite").format("delta").save(acc_trusted_path)
print(f"Accelerometer data written to {acc_trusted_path}")


# COMMAND ----------

acc_trusted_df = spark.read.format("delta").load('/mnt/stedi/accelerometer/trusted/')
display(f'Trusted path total customers: {acc_trusted_df.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Trusted Accelerometer Table in Catalog for Queries

# COMMAND ----------


spark.sql("""
          CREATE TABLE IF NOT EXISTS stedi.accelerometer_trusted
          USING delta
          OPTIONS (path "/mnt/stedi/accelerometer/trusted/")""")