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

source_file = f"{landing_folder_path}/{v_batch_id}/sprints"
target_table = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
sprints_schema = StructType([
    StructField("date", DateType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
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
    StructField("status", StringType())    
])

# COMMAND ----------

sprints_df = (spark.read
              .format('json')
              .option('MultiLine','true')
              .schema(sprints_schema)
              .load(source_file)
)
              

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

sprints_final_df = add_ingest_metadata(sprints_df)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# (sprints_final_df
#  .write
#  .format("delta")
#  .mode('overwrite')
#  .saveAsTable(target_table)
#  )
write_to_bronze(
    input_df = sprints_final_df,
    target_table = target_table,
    batch_id = v_batch_id)


# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG formula1_incr;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC select * from sprints
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sprints