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

bronze_sprints_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 Read bronze sprints table

# COMMAND ----------

sprints_df = (
    spark.read
    .table(bronze_sprints_table)
    .filter(f.col("batch_id") == v_batch_id)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 Exclude URL column from dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as f
sprints_selected_df = sprints_df.select(
    f.col("date"),
    f.col("raceName"),
    f.col("round"),
    f.col("season"),
    f.col("constructorId"),
    f.col("driverId"),
    f.col("grid"),
    f.col("laps"),
    f.col("number"),
    f.col("points"),
    f.col("position"),
    f.col("positionText"),
    f.col("status"),
    f.col("ingested_timestamp"),
    f.col("source_file"),
    f.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4 Standardize the column names**

# COMMAND ----------

# DBTITLE 1,Step 4 Standardize the column names
sprints_renamed_df = (sprints_selected_df
                    #    .withColumnRenamed('circuitId',"circuit_id")
                    #    .withColumnRenamed("circuitName","circuit_name")
                    #    .withColumnRenamed("lat","latitude")
                    #    .withColumnRenamed("long","longitude")
                    .withColumnsRenamed({"driverId":"driver_id",
                                        "constructorId":"constructor_id",
                                        "raceName":"race_name",
                                        "date":"race_date",
                                        "grid":"grid_position",
                                        "laps":"laps_completed",
                                        "number":"car_number",
                                        "position":"final_position",
                                        "positionText":"final_position_text"})
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5 Filter out null sprints records**

# COMMAND ----------

sprints_valid_df =( sprints_renamed_df
                    # .filter("circuit_id is not null")
                    .filter(
                    f.col("driver_id").isNotNull() &
                    f.col("constructor_id").isNotNull() &
                    f.col("season").isNotNull() &
                    f.col("round").isNotNull()) 
                     )                   
                    

# COMMAND ----------

display(sprints_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 Removed duplicates**

# COMMAND ----------

# sprints_distinct_df = sprints_valid_df.distinct() removes the duplicated only if all the column records are same
sprints_distinct_df = sprints_valid_df.dropDuplicates(['season','round','driver_id',"constructor_id"])
# Option to give list columns to remove duplicates

# COMMAND ----------

display(sprints_distinct_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7 Tranform column race_name to title case**

# COMMAND ----------

sprints_final_df = (
    sprints_distinct_df
.withColumn("race_name", f.initcap(f.col("race_name")))
)


# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8 Write data to silver sprints table**

# COMMAND ----------

write_to_silver (
    input_df = sprints_final_df,
    target_table = silver_table,
    merge_condition = "s.season = t.season AND s.round = t.round AND s.driver_id = t.driver_id AND s.constructor_id = t.constructor_id",
    columns_to_update= ["race_date", "race_name", "round", "season", "constructor_id", "driver_id", "grid_position", "laps_completed", "car_number", "points", "final_position", "final_position_text", "status", "batch_id","ingested_timestamp"]
)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG formula1_incr;
# MAGIC USE SCHEMA Silver;
# MAGIC
# MAGIC select * from sprints
# MAGIC