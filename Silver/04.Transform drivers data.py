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

bronze_drivers_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 Read bronze drivers table

# COMMAND ----------

drivers_df = (spark.read
              .table(bronze_drivers_table)
              .filter(f.col("batch_id") == v_batch_id)
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 Exclude URL column from dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as f
drivers_selected_df = drivers_df.select(
    f.col("driverId"),
    f.col("name"),
    f.col("dateOfBirth"),
    f.col("nationality"),
    f.col("ingested_timestamp"),
    f.col("source_file"),
    f.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4 Standardize the column names**

# COMMAND ----------

# DBTITLE 1,Step 4 Standardize the column names
drivers_renamed_df = (drivers_selected_df
                    #    .withColumnRenamed('circuitId',"circuit_id")
                    #    .withColumnRenamed("circuitName","circuit_name")
                    #    .withColumnRenamed("lat","latitude")
                    #    .withColumnRenamed("long","longitude")
                    .withColumnRenamed("driverId","driver_id")
                    .withColumn("driver_name",f.initcap(f.concat_ws(" ",f.col("name.givenname"),f.col("name.familyname")))
                      )
                    .drop("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5 Filter out null drivers records**

# COMMAND ----------

drivers_valid_df =( drivers_renamed_df
                    # .filter("circuit_id is not null")
                    .filter(f.col("driver_id").isNotNull())
                    )
                    

# COMMAND ----------

display(drivers_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 Removed duplicates**

# COMMAND ----------

# drivers_distinct_df = drivers_valid_df.distinct() removes the duplicated only if all the column records are same
drivers_distinct_df = drivers_valid_df.dropDuplicates() # Option to give list columns to remove duplicates

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7 Tranform column driver_name to title case**

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
.withColumn("driver_name", f.initcap(f.col("driver_name")))
)


# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8 Write data to silver drivers table**

# COMMAND ----------

write_to_silver (
    input_df = drivers_final_df,
    target_table = silver_table,
    merge_condition = "s.driver_id = t.driver_id",
    columns_to_update = ["driver_name", "driver_id", "dateOfBirth", "nationality","ingested_timestamp","source_file","batch_id"]
)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG formula1_incr;
# MAGIC USE SCHEMA Silver;
# MAGIC
# MAGIC select * from drivers
# MAGIC
# MAGIC