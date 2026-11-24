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
# MAGIC ### Step 1 Read silver constructor and gold nationality region table. Create table variable for dim constructor

# COMMAND ----------

constructor_df = (
spark
        .table(f"{catalog_name}.{silver_schema}.constructors")
        .filter(f.col("batch_id") == v_batch_id)
        )
nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.nationality_region")

gold_table = f"{catalog_name}.{gold_schema}.dim_constructor"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 Join constructor with nationality table and select only required columns

# COMMAND ----------

dim_constructor_df =( constructor_df
                     .join(nationality_region_df,
                           nationality_region_df.nationality == constructor_df.nationality,
                           'left')
                      .select(constructor_df.constructor_id,
                             constructor_df.constructor_name,
                             nationality_region_df.nationality,
                             nationality_region_df.region.alias('nationality_region'))
)

# COMMAND ----------

display(dim_constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 Load to dim constructor table

# COMMAND ----------

write_to_gold (
        input_df = dim_constructor_df,
        target_table = gold_table,
        merge_condition = "t.constructor_id = s.constructor_id",
        columns_to_update = ["constructor_id",
                             "constructor_name",
                             "nationality","nationality_region"]

)

# COMMAND ----------

display(spark.table(gold_table))