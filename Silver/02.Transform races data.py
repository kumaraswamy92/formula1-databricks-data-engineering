# Databricks notebook source
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

# MAGIC %run ../00-common/03.Silver-Helper

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create parameter to catch batch ![id](path)

# COMMAND ----------

dbutils.widgets.text("batch_id","")
v_batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 create table variables

# COMMAND ----------

bronze_races_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 Read bronze races table

# COMMAND ----------

races_df = (spark.read
            .table(bronze_races_table)
            .filter(f.col("batch_id") == v_batch_id)
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 Exclude URL column from dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as f
races_selected_df = races_df.select(
    f.col("season"),
    f.col("round"),
    f.col("raceName"),
    f.col("date"),
    f.col("circuitid"),
    f.col("ingested_timestamp"),
    f.col("source_file"),
    f.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4 Standardize the column names**

# COMMAND ----------

races_renamed_df = (races_selected_df
                    #    .withColumnRenamed('circuitId',"circuit_id")
                    #    .withColumnRenamed("circuitName","circuit_name")
                    #    .withColumnRenamed("lat","latitude")
                    #    .withColumnRenamed("long","longitude")
                    .withColumnsRenamed(
                        {"circuitId":"circuit_id",
                         "racename":"race_name",
                         "date":"race_date"}
                      )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5 Filter out null races records**

# COMMAND ----------

races_valid_df =( races_renamed_df
                    # .filter("circuit_id is not null")
                    .filter(f.col("circuit_id").isNotNull())
                    )
                    

# COMMAND ----------

display(races_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 Removed duplicates**

# COMMAND ----------

# races_distinct_df = races_valid_df.distinct() removes the duplicated only if all the column records are same
races_distinct_df = races_valid_df.dropDuplicates() # Option to give list columns to remove duplicates

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7 Tranform column race_name to title case**

# COMMAND ----------

races_final_df = (
    races_distinct_df
.withColumn("race_name", f.initcap(f.col("race_name")))
)


# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8 Write data to silver races table**

# COMMAND ----------

write_to_silver (
    input_df = races_final_df,
    target_table = silver_table,
    merge_condition = "t.circuit_id = s.circuit_id and t.season = s.season   and t.round = s.round",
    columns_to_update =[ "season", "round", "race_name", "race_date","circuit_id","ingested_timestamp","source_file","batch_id"]
)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog formula1_incr;
# MAGIC use schema silver;
# MAGIC
# MAGIC select circuit_id, season from races where season=2020
# MAGIC order by circuit_id, season