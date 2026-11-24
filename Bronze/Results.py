# Databricks notebook source
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

# MAGIC %run ../00-common/02.Bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create parameter to catch batch id

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")

v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/results"
target_table = f"{catalog_name}.{bronze_schema}.results"


# COMMAND ----------

from pyspark.sql.types import  StructField, StructType, StringType, IntegerType, DateType, DoubleType
results_schema = StructType(
                        [
                                   StructField("date", DateType()),
                                   StructField("raceName", StringType()),
                                   StructField("round", StringType()),
                                   StructField("season", IntegerType()),
                                   StructField("url", StringType()),
                                   StructField("constructorId", StringType()),
                                   StructField("driverId", StringType()),
                                   StructField("grid", StringType()),
                                   StructField("laps", IntegerType()),
                                   StructField("number", IntegerType()),
                                   StructField("points", DoubleType()),
                                   StructField("position", IntegerType()),
                                   StructField("positionText", StringType()),
                                   StructField("status", StringType()),
                        ]
)

# COMMAND ----------

results_df = (spark.read
                  .format('json')
                  .schema(results_schema)
                  .load(source_file)
                  )

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_final_df = add_ingest_metadata(results_df)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# (results_final_df 
#     .write
#     .mode('overwrite')
#     .format('delta')    
#     .saveAsTable(target_table)
# )
write_to_bronze(
    input_df = results_final_df,
    target_table = target_table,
    batch_id = v_batch_id)

# COMMAND ----------

display(spark.table(target_table))