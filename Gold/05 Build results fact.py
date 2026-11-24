# Databricks notebook source
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %run ../00-common/04.Gold-Helper

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create parameter to catch batch ![id](path)

# COMMAND ----------

dbutils.widgets.text("batch_id","")
v_batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 Read silver results and sprints data and add race type column with default value and drop unwanted columns

# COMMAND ----------

results_df = (spark
              .table(f"{catalog_name}.{silver_schema}.results")
              .withColumn("race_type",f.lit("RACE"))
              .filter(f.col("batch_id") == v_batch_id)
              .drop("race_name","race_date","ingested_timestamp","source_file","created_timestamp","updated_timestamp"))
sprints_df = (spark
                .table(f"{catalog_name}.{silver_schema}.sprints")
                .withColumn("race_type",f.lit("SPRINT"))
                .filter(f.col("batch_id") == v_batch_id)
                .drop("race_name","race_date","ingested_timestamp","source_file","created_timestamp","updated_timestamp"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 Create fact_session_results table

# COMMAND ----------

gold_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 union results and sprints dataframe

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 Add columns is_win, is_podium, has_points

# COMMAND ----------

fact_session_df =  (results_sprints_df
                 .withColumn("is_win",f.col("final_position") == 1 )
                 .withColumn("is_podium", f.col("final_position") .between(1,3))
                 .withColumn("has_point", f.col("points") >0)         
                  )

# COMMAND ----------

display(fact_session_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 Write to target table

# COMMAND ----------

write_to_gold (
        input_df = fact_session_df,
        target_table = gold_table,
        merge_condition = "t.race_type = s.race_type and t.driver_id = s.driver_id and t.season = s.season and t.round = s.round and t.constructor_id = s.constructor_id",
        columns_to_update = ['round',
 'season',
 'constructor_id',
 'driver_id',
 'grid_position',
 'laps_completed',
 'car_number',
 'points',
 'final_position',
 'final_position_text',
 'status',
  'race_type','is_win',
 'is_podium',
 'has_point']
)

# COMMAND ----------

display(spark.table(gold_table))