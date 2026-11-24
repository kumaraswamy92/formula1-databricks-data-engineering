# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest race.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read CSV file using using dataframe reader API

# COMMAND ----------

# MAGIC %md
# MAGIC # Running the created env config to use in the dataframe
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

# MAGIC %md
# MAGIC # Calling the user created function add_ingest_metadata into this notebook

# COMMAND ----------

# MAGIC %run ../00-common/02.Bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create parameter to catch batch id

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")

v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

Race_schema = StructType([
    StructField('season', StringType()),
    StructField('round', StringType()),
    StructField('url', StringType()),
    StructField('raceName', StringType()),
    StructField('date', TimestampType()),
    StructField('circuitId', StringType())
])


# COMMAND ----------

source_file_race = f"{landing_folder_path}/{v_batch_id}/races.csv"
target_table = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

Race_df = (
    spark.read
    .format('csv')
    .option('header','true')
    .schema(Race_schema)
    .load(source_file_race)
    #.load('/Volumes/formula1/landing/files/formula1-full-load-landing/landing/races.csv')
)

# COMMAND ----------

display(Race_df)

# COMMAND ----------


# from pyspark.sql import functions as F
# Race_Final_Df = (
#     Race_df 
#     .withColumn('ingested_timestamp',F.current_timestamp())
#     .withColumn('source_file',F.col('_metadata.file_path'))
# )

Race_Final_Df = add_ingest_metadata(Race_df)

# COMMAND ----------

display(Race_Final_Df)

# COMMAND ----------

# (
#     Race_Final_Df
#     .write
#     .format('delta')
#     .mode('overwrite')
#     .saveAsTable(table_name_race)
# )

write_to_bronze (
    input_df = Race_Final_Df,
    target_table = target_table,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(target_table))