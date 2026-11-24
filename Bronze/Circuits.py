# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuit.csv file

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

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

Circuits_schema = StructType([
    StructField('circuitId', StringType()),
    StructField('url', StringType()),
    StructField('circuitName', StringType()),
    StructField('lat', DoubleType()),
    StructField('long', DoubleType()),
    StructField('locality', StringType()),
    StructField('country', StringType())
])

# COMMAND ----------

source_file_circuits = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
target_table = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

from pyspark.sql import functions as f

Circuits_df = (
    spark.read    
    .format('csv')
    .option('header','true')
    #.option('inferSchema','true')
    .schema(Circuits_schema)
    .option('mode','FailFast')
    .load(source_file_circuits)
    #.load('/Volumes/formula1/landing/files/formula1-full-load-landing/landing/circuits.csv')
)

# COMMAND ----------

display(Circuits_df)

# COMMAND ----------

# from pyspark.sql import functions as F
# Circuits_Final_df = (
#  Circuits_df
#  .withColumn('ingested_timestamp',F.current_timestamp())
#  .withColumn('source_file',F.col('_metadata.file_path'))
# )

Circuits_Final_df = add_ingest_metadata(Circuits_df)

# COMMAND ----------

display(Circuits_Final_df)


# COMMAND ----------


# (Circuits_Final_df
#     .write
#     .mode('overwrite')
#     .format('delta')
#     .partitionBy('batch_id')
#     .option('replaceWhere',f"batch_id = '{v_batch_id}'")
#     .saveAsTable(table_name_circuits)
# )

write_to_bronze(
    input_df = Circuits_Final_df,
    target_table = target_table,
    batch_id = v_batch_id)

# COMMAND ----------


display(spark.table(target_table))


