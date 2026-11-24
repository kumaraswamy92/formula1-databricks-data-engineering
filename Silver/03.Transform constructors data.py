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

bronze_constructors_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 Read bronze constructors table

# COMMAND ----------

constructors_df = (
     spark.read
    .table(bronze_constructors_table)
    .filter(f.col("batch_id") == v_batch_id)
    )


# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 Exclude URL column from dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as f
constructors_selected_df = constructors_df.select(
    f.col("constructorId"),
    f.col("name"),
    f.col("nationality"),
    f.col("ingested_timestamp"),
    f.col("source_file"),
    f.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4 Standardize the column names**

# COMMAND ----------

constructors_renamed_df = (constructors_selected_df
                    #    .withColumnRenamed('circuitId',"circuit_id")
                    #    .withColumnRenamed("circuitName","circuit_name")
                    #    .withColumnRenamed("lat","latitude")
                    #    .withColumnRenamed("long","longitude")
                    .withColumnsRenamed(
                        {"constructorId":"constructor_id",
                         "name":"constructor_name"}
                      )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5 Filter out null constructors records**

# COMMAND ----------

constructors_valid_df =( constructors_renamed_df
                    # .filter("circuit_id is not null")
                    .filter(f.col("constructor_id").isNotNull())
                    )
                    

# COMMAND ----------

display(constructors_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 Removed duplicates**

# COMMAND ----------

# constructors_distinct_df = constructors_valid_df.distinct() removes the duplicated only if all the column records are same
constructors_distinct_df = constructors_valid_df.dropDuplicates() # Option to give list columns to remove duplicates

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7 Tranform column constructor_name to title case**

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
.withColumn("constructor_name", f.initcap(f.col("constructor_name")))
)


# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8 Write data to silver constructors table**

# COMMAND ----------

write_to_silver (
    input_df = constructors_final_df,
    target_table = silver_table,
    merge_condition = "s.constructor_id = t.constructor_id",
    columns_to_update = ["constructor_name","constructor_id","nationality","ingested_timestamp","source_file","batch_id"]
)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG formula1_incr;
# MAGIC USE SCHEMA Silver;
# MAGIC
# MAGIC select * from constructors
# MAGIC