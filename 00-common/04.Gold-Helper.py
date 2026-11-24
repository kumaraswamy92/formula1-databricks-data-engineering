# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

def write_to_gold (
    input_df,
    target_table,
    merge_condition,
    columns_to_update
):
    final_df = (
        input_df
        .withColumn('created_timestamp', f.current_timestamp())
        .withColumn('updated_timestamp',f.current_timestamp())
                    )
    if not spark.catalog.tableExists(gold_table):
        (
            final_df
            .write
            .format('delta')
            .mode('overwrite')
            .saveAsTable(gold_table)
        )
    else:
        delta_table = DeltaTable.forName(spark, gold_table)
        update_map ={column: f"s.{column}" for column in columns_to_update }
        update_map["updated_timestamp"] = "s.updated_timestamp"

        (delta_table.alias("t")
        .merge(
            final_df.alias("s"),
            merge_condition
        )
        .whenMatchedUpdate(
            set=update_map)
        .whenNotMatchedInsertAll()
        .execute()
        )
    