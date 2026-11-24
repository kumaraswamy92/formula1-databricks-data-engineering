# Databricks notebook source
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

control_table =f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

if v_batch_id:
    delta_table = DeltaTable.forName(spark, control_table)

    source_df = (
        spark.createDataFrame([(v_batch_id,)], ["batch_id"])
        .withColumn("updated_timestamp", F.current_timestamp())
        .withColumn("status", F.lit("completed"))
    )

    (
        delta_table.alias("t")
        .merge(source_df.alias("s"), 
        "t.batch_id = s.batch_id and t.status='in_progress'"
        )
        .whenMatchedUpdate (
            set ={
                "status": "s.status",
                "updated_timestamp": "s.updated_timestamp"
            }
        )
        .execute()
    )

    print (f"Batch {v_batch_id} marked as completed")
else:
    raise Exception("batch is missing")
    


