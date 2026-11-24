# Databricks notebook source
# MAGIC %md
# MAGIC Ingest constructor JSON

# COMMAND ----------

# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

# MAGIC %run ../00-common/02.Bronze-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create parameter to catch batch [id](url)

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")

v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# DBTITLE 1,aget
source_file = f"{landing_folder_path}/{v_batch_id}/constructors.json"
target_table = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

display(source_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Step 1 Read JSON file using dataframe API

# COMMAND ----------

# Define schema

contructors_schema = """
  constructorId STRING,
  name STRING,
  nationality STRING,
  url STRING"""

# COMMAND ----------

# Read data from constructor file
constructors_df = (
 spark.read
    .format('json')
    .schema(contructors_schema)
    .option('mode','FailFast')
    .load(source_file)
)

# COMMAND ----------

constructors_final_df = add_ingest_metadata(constructors_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# (constructors_final_df
#  .write
#  .format('delta')
#  .mode('overwrite')
#  .saveAsTable(target_table)
# )

write_to_bronze(
    input_df = constructors_final_df,
    target_table = target_table,
    batch_id = v_batch_id)


# COMMAND ----------

display(spark.table(target_table))