# Databricks notebook source
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

# MAGIC %run ../00-common/04.Gold-Helper

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create parameter to catch batch ![id](path)

# COMMAND ----------

dbutils.widgets.text("batch_id","")
v_batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 Read silver drivers data and ationality_region data

# COMMAND ----------

driver_df = (spark.table(f"{catalog_name}.{silver_schema}.drivers")
             .filter(f.col("batch_id") == v_batch_id)
)
nationality_df = spark.table(f"{catalog_name}.{gold_schema}.nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 Create target table dim_drivers

# COMMAND ----------

gold_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 Join driver and nationality tables select required columns

# COMMAND ----------

dim_driver_df = (driver_df
                .join (nationality_df,
                       driver_df.nationality == nationality_df.nationality,
                       "left")
                .select(driver_df.driver_id,
                         driver_df.driver_name,
                        driver_df.dateOfBirth,
                        f.initcap(driver_df.nationality).alias("nationality"),
                        nationality_df.region)
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 load to dim_drivers table

# COMMAND ----------

write_to_gold (
    input_df = dim_driver_df,
    target_table = gold_table,
    merge_condition = "t.driver_id = s.driver_id",
    columns_to_update = ["driver_id",
                         "driver_name",
                         "dateOfBirth",
                         "nationality",
                         "region"]
    
)

# COMMAND ----------

display(spark.table(gold_table))