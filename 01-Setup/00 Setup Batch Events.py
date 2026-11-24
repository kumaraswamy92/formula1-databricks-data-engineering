# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists formula1.control

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists formula1.control.batch_events
# MAGIC ( 
# MAGIC     ID INT,
# MAGIC     EVENT_TIMESTAMP TIMESTAMP
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into formula1.control.batch_events values(1,current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.control.batch_events