# Databricks notebook source
# MAGIC %md
# MAGIC ## Set Up Step Trainer Paths to Read and Generate DF

# COMMAND ----------


# Path to customer landing JSON files
st_landing_path = "/mnt/stedi/step-trainer/landing/"

# Read all JSON files in the folder
st_landing_df = spark.read.json(st_landing_path)

# Show schema to verify fields
st_landing_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Step Trainer Temp Table 

# COMMAND ----------

st_landing_df.createOrReplaceTempView("st_landing_temp")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM st_landing_temp
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Step Trainer Trusted Table Filtered against Customer Trusted

# COMMAND ----------

spark.catalog.dropTempView("st_trusted")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW st_trusted AS
# MAGIC SELECT 
# MAGIC   st.*
# MAGIC FROM st_landing_temp as st
# MAGIC INNER JOIN stedi.customer_curated as c
# MAGIC ON st.serialNumber = c.serialNumber;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM st_trusted;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write New Dataframe to Trusted (Silver) Zone

# COMMAND ----------

st_trusted_path = "/mnt/stedi/step-trainer/trusted/"

spark.table("st_trusted").write.mode("overwrite").format("delta").save(st_trusted_path)
print(f"Step Trainer data written to {st_trusted_path}")


# COMMAND ----------

st_trusted_df = spark.read.format('delta').load('/mnt/stedi/step-trainer/trusted/')
display(f'Trusted path total customers: {st_trusted_df.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Trusted Accelerometer Table in Catalog for Queries

# COMMAND ----------


spark.sql("""
          CREATE TABLE IF NOT EXISTS stedi.step_trainer_trusted
          USING delta
          OPTIONS (path "/mnt/stedi/step-trainer/trusted/")""")