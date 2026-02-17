# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up Customer Paths to Read and Generate DF

# COMMAND ----------


# Path to customer landing JSON files
customer_landing_path = "/mnt/stedi/customer/landing/"

# Read all JSON files in the folder
customer_df = spark.read.json(customer_landing_path)

# Show schema to verify fields
customer_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter Customer Based on Share with Research Consent

# COMMAND ----------

filtered_df = customer_df.filter(
    col("shareWithResearchAsOfDate").cast(IntegerType()).isNotNull()
)


# COMMAND ----------

display(filtered_df)

# Optional: count rows
print("Number of customers with shareWithResearchAsOfDate:", filtered_df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write New Dataframe to Trusted (Silver) Zone

# COMMAND ----------

trusted_path = "/mnt/stedi/customer/trusted/"

# Write as Parquet
filtered_df.write.mode("overwrite").format("delta").save(trusted_path)

print(f"Filtered customer data written to {trusted_path}")

# COMMAND ----------

trusted_df = spark.read.format('delta').load(trusted_path)
display(f'Trusted path total customers: {trusted_df.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Trusted Customer Table in Catalog for Queries

# COMMAND ----------


spark.sql("""
          CREATE TABLE IF NOT EXISTS stedi.customer_trusted
          USING delta
          OPTIONS (path "/mnt/stedi/customer/trusted/")""")