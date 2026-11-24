# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog formula1_incr

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists formula1_incr.bronze;
# MAGIC create schema if not exists formula1_incr.silver;
# MAGIC create schema if not exists formula1_incr.gold;
# MAGIC
# MAGIC create schema if not exists formula1_incr.landing;

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists  formula1_incr.landing.files