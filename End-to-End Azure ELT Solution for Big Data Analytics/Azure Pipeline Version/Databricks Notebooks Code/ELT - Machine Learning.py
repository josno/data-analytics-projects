# Databricks notebook source
# MAGIC %md
# MAGIC ## Join Step Trainer Trusted and Accelerometer Trusted Tables

# COMMAND ----------

spark.catalog.dropTempView("ml_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW ml_temp AS
# MAGIC SELECT 
# MAGIC   stt.distanceFromObject,
# MAGIC   stt.serialNumber, -- Selecting all stt columns minus sensorReadingTime because it duplicates a.timestamp
# MAGIC   a.*
# MAGIC FROM stedi.accelerometer_trusted as a
# MAGIC INNER JOIN stedi.step_trainer_trusted as stt
# MAGIC ON a.timestamp = stt.sensorReadingTime;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM ml_temp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write New Dataframe to Curated (Gold) Zone

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE ml_temp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write ML Data to Path
# MAGIC

# COMMAND ----------

ml_path = "/mnt/stedi/machine-learning/curated/"

spark.table("ml_temp").write.mode("overwrite").format("delta").save(ml_path)
print(f"ML data written to {ml_path}")


# COMMAND ----------

ml_df = spark.read.format("delta").load('/mnt/stedi/machine-learning/curated/')
display(f'Trusted path total customers: {ml_df.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Trusted Accelerometer Table in Catalog for Queries

# COMMAND ----------


spark.sql("""
          CREATE TABLE IF NOT EXISTS stedi.machine_learning
          USING delta
          OPTIONS (path "/mnt/stedi/machine-learning/curated/")""")