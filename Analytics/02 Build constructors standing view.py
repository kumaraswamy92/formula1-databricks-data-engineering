# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW FORMULA1.GOLD.vw_constuctors_standings
# MAGIC AS
# MAGIC WITH CONSTRUCTORS_SESSION_SUMMARY
# MAGIC AS (
# MAGIC SELECT R.season,
# MAGIC     D.constructor_id,
# MAGIC     D.constructor_name,
# MAGIC     nationality,
# MAGIC     count(*) as race_starts,
# MAGIC     sum(r.points) as total_pointS,
# MAGIC     count_if(r.is_win) AS NO_OF_WINS,
# MAGIC     count_if(r.is_podium) AS NO_OF_PODIUM
# MAGIC      FROM FORMULA1.GOLD.fact_session_results R
# MAGIC INNER JOIN FORMULA1.GOLD.dim_constructor D
# MAGIC ON R.constructor_id = D.constructor_id
# MAGIC GROUP BY  R.season,
# MAGIC     D.constructor_id,
# MAGIC     D.constructor_name,
# MAGIC     nationality)
# MAGIC SELECT season,
# MAGIC     constructor_id,
# MAGIC     constructor_name,
# MAGIC     nationality,
# MAGIC     race_starts,
# MAGIC     total_pointS,
# MAGIC     NO_OF_WINS,
# MAGIC     NO_OF_PODIUM,
# MAGIC     RANK() OVER (PARTITION BY SEASON ORDER BY TOTAL_POINTS DESC, NO_OF_WINS DESC, NO_OF_PODIUM DESC) AS standing_position
# MAGIC     FROM CONSTRUCTORS_SESSION_SUMMARY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FORMULA1.GOLD.vw_constuctors_standings
# MAGIC WHERE SEASON=2025