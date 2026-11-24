# Databricks notebook source
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

spark.sql(f" create schema if not exists {catalog_name}.{control_schema}")

# COMMAND ----------

spark.sql (f"""
           create table if not exists {catalog_name}.{control_schema}.batch_control
           (    batch_id string,
                status string,
                created_timestamp timestamp,
                updated_timestamp timestamp
           )
           """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select datediff(minute,created_timestamp,updated_timestamp),a.* from formula1_incr.control.batch_control a
# MAGIC order by 2
# MAGIC