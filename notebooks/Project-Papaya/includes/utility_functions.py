# Databricks notebook source
# MAGIC %md
# MAGIC ##Utility functions for other files

# COMMAND ----------

#All imports
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

#Function to add ingestion_date to dataframe
def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#Function to add datasource column
def add_data_source(input_df, data_source):
    return input_df.withColumn("data_source", lit(data_source))