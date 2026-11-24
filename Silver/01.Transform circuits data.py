# Databricks notebook source
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

# MAGIC %run ../00-common/03.Silver-Helper
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create parameter to catch batch ![id](path)

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")

v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 create table variables

# COMMAND ----------

bronze_circuit_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 Read bronze circuits table

# COMMAND ----------

circuits_df = (spark.read
               .table(bronze_circuit_table)
               .filter(f.col("batch_id") == v_batch_id)
               )

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 Exclude URL column from dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as f
circuits_selected_df = circuits_df.select(
    f.col("circuitId"),
    f.col("circuitName"),
    f.col("lat"),
    f.col("long"),
    f.col("locality"),
    f.col("country"),
    f.col("ingested_timestamp"),
    f.col("source_file"),
    f.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4 Standardize the column names**

# COMMAND ----------

circuits_renamed_df = (circuits_selected_df
                    #    .withColumnRenamed('circuitId',"circuit_id")
                    #    .withColumnRenamed("circuitName","circuit_name")
                    #    .withColumnRenamed("lat","latitude")
                    #    .withColumnRenamed("long","longitude")
                    .withColumnsRenamed(
                        {"circuitId":"circuit_id",
                         "circuitName":"circuit_name",
                         "lat":"latitude",
                         "long":"longitude"}
                      )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5 Filter out null circuit records**

# COMMAND ----------

circuits_valid_df =( circuits_renamed_df
                    # .filter("circuit_id is not null")
                    .filter(f.col("circuit_id").isNotNull())
                    )
                    

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 Removed duplicates**

# COMMAND ----------

# circuits_distinct_df = circuits_valid_df.distinct() removes the duplicated only if all the column records are same
circuits_distinct_df = circuits_valid_df.dropDuplicates() # Option to give list columns to remove duplicates

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7 Tranform columns circuit_name and locality to title case**

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
.withColumn("circuit_name", f.initcap(f.col("circuit_name")))
.withColumn("locality", f.initcap(f.col("locality")))
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# from delta.tables import DeltaTable

# if not spark.catalog.tableExists(silver_table):
#     (circuits_final_df
#      .write
#      .format('delta')
#      .mode('overwrite')
#      .saveAsTable(silver_table))
# else:
#     delta_table = DeltaTable.forName(spark, silver_table)
#     (
#         delta_table.alias("t")
#         .merge(circuits_final_df.alias("s"),
#                "t.circuit_id = s.circuit_id")
#         .whenMatchedUpdate(
#             condition ="s.batch_id >= t.batch_id",
#             set = { "circuit_name" : "s.circuit_name",
#                   "latitude" : "s.latitude",
#                   "longitude" : "s.longitude",
#                   "locality" : "s.locality",
#                   "country" : "s.country",
#                   "ingested_timestamp" : "s.ingested_timestamp",
#                   "source_file" : "s.source_file",
#                   "batch_id" : "s.batch_id",
#                   "updated_timestamp" : "s.updated_timestamp"
#             }
#         )
#         .whenNotMatchedInsertAll()
#         .execute()
#     )


# COMMAND ----------

write_to_silver (
    input_df = circuits_final_df,
    target_table = silver_table,
    merge_condition = "t.circuit_id = s.circuit_id",
    columns_to_update = ["circuit_name", "latitude", "longitude", "locality", "country", "ingested_timestamp", "source_file", "batch_id"]
)

# COMMAND ----------

display(spark.table(silver_table))
