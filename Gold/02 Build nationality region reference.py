# Databricks notebook source
# DBTITLE 1,Notebook title
# MAGIC %md
# MAGIC # Build nationality region reference
# MAGIC
# MAGIC Creates the `nationality_region` reference table in the gold layer, mapping every distinct driver nationality from the silver drivers table to its world region.

# COMMAND ----------

# DBTITLE 1,Environment configs
# MAGIC %run ../00-common/01.Environment-configs

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ### Step 1 Define the reference data and target table

# COMMAND ----------

# DBTITLE 1,Define reference data
nationality_region_table = f"{catalog_name}.{gold_schema}.nationality_region"

# Values are lowercase to match the silver drivers table exactly
nationality_region_data = [
    ("american", "North America"),
    ("argentine", "South America"),
    ("australian", "Oceania"),
    ("austrian", "Europe"),
    ("belgian", "Europe"),
    ("brazilian", "South America"),
    ("british", "Europe"),
    ("canadian", "North America"),
    ("chilean", "South America"),
    ("chinese", "Asia"),
    ("colombian", "South America"),
    ("czech", "Europe"),
    ("danish", "Europe"),
    ("dutch", "Europe"),
    ("east german", "Europe"),
    ("finnish", "Europe"),
    ("french", "Europe"),
    ("german", "Europe"),
    ("hungarian", "Europe"),
    ("indian", "Asia"),
    ("indonesian", "Asia"),
    ("irish", "Europe"),
    ("italian", "Europe"),
    ("japanese", "Asia"),
    ("liechtensteiner", "Europe"),
    ("malaysian", "Asia"),
    ("mexican", "North America"),
    ("monegasque", "Europe"),
    ("new zealander", "Oceania"),
    ("polish", "Europe"),
    ("portuguese", "Europe"),
    ("rhodesian", "Africa"),
    ("russian", "Europe"),
    ("south african", "Africa"),
    ("spanish", "Europe"),
    ("swedish", "Europe"),
    ("swiss", "Europe"),
    ("thai", "Asia"),
    ("uruguayan", "South America"),
    ("venezuelan", "South America"),
]

nationality_region_df = spark.createDataFrame(
    nationality_region_data,
    schema="nationality string, region string"
)

display(nationality_region_df)

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ### Step 2 Load the reference data into the gold table

# COMMAND ----------

# DBTITLE 1,Write to gold table
(nationality_region_df
 .write
 .format('delta')
 .mode('overwrite')
 .saveAsTable(nationality_region_table)
)

display(spark.table(nationality_region_table))

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ### Step 3 Validate coverage against the silver drivers table

# COMMAND ----------

# DBTITLE 1,Validate coverage
from pyspark.sql import functions as f

silver_nationalities_df = (
    spark.table(f"{catalog_name}.{silver_schema}.drivers")
    .select("nationality")
    .where(f.col("nationality").isNotNull())
    .distinct()
)

# Nationalities present in silver but missing from the reference (should be empty)
missing_nationalities_df = silver_nationalities_df.join(
    nationality_region_df, "nationality", "left_anti"
)

display(missing_nationalities_df)