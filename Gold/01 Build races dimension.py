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
# MAGIC ### Step 1 Read silver layer circuits and races tables

# COMMAND ----------

circuits_df = (
    spark
    .table(f"{catalog_name}.{silver_schema}.circuits")
    .filter(f.col("batch_id") == v_batch_id)
    )
races_df = (
    spark
    .table(f"{catalog_name}.{silver_schema}.races")
    .filter(f.col("batch_id") == v_batch_id)
)
# create target table
gold_table = f"{catalog_name}.{gold_schema}.dim_race"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 Join races and circuits tables

# COMMAND ----------

dim_race_df = ( circuits_df
          .join(races_df,
                races_df.circuit_id == circuits_df.circuit_id,
                "inner"
               )
          .select(races_df.season,
                  races_df.round,
                  races_df.race_name,
                  races_df.race_date,
                  circuits_df.circuit_name,
                  circuits_df.locality,
                  circuits_df.country)
               )

# COMMAND ----------

display(dim_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 Load dataframe to gold layer dim race table

# COMMAND ----------

write_to_gold (
    input_df = dim_race_df,
    target_table = gold_table,
    merge_condition = "t.season = s.season AND t.round = s.round",
    columns_to_update= ["season",
                  "round",
                  "race_name",
                  "race_date",
                  "circuit_name",
                  "locality",
                  "country"]
)

# COMMAND ----------

display(spark.table(gold_table))