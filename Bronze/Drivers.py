# Databricks notebook source
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

# MAGIC %run ../00-common/02.Bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create parameter to add batch id

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")

v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/drivers.json"
target_table = f"{catalog_name}.{bronze_schema}.drivers"


# COMMAND ----------

display(source_file)

# COMMAND ----------

# DBTITLE 1,Define driver schema
from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([
    StructField("givenName" , StringType()),
    StructField("familyName" , StringType()),
])

drivers_schema = StructType([
    StructField("driverId" , StringType()),
    StructField("name" , name_schema),
    StructField("dateOfBirth" , DateType()),
    StructField("nationality" , StringType()),
    StructField("url" , StringType())
])

# COMMAND ----------

drivers_df = (spark.read
            .format('json')
            .schema(drivers_schema)
            .option('mode','FastFail')
            .load(source_file)
            )

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_final_df = add_ingest_metadata(drivers_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# (drivers_final_df
#         .write
#         .mode('overwrite')
#         .format('delta')
#         .saveAsTable(target_table)
# )

write_to_bronze(
    input_df = drivers_final_df,
    target_table = target_table,
    batch_id = v_batch_id)

# COMMAND ----------

display(spark.table(target_table))