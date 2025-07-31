# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_new_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_new_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - create our schema and read file
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                  StructField("driverId", IntegerType(), True),
                                  StructField("constructorId", IntegerType(), True),
                                  StructField("number", IntegerType(), True),
                                  StructField("grid", IntegerType(), True),
                                  StructField("position", IntegerType(), True),
                                  StructField("positionText", StringType(), True),
                                  StructField("positionOrder", IntegerType(), True),
                                  StructField("points", IntegerType(), True),
                                  StructField("laps", IntegerType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("milliseconds", IntegerType(), True),
                                  StructField("fastestLap", IntegerType(), True),
                                  StructField("rank", IntegerType(), True),
                                  StructField("fastestLapTime", StringType(), True),
                                  StructField("fastestLapSpeed", StringType(), True),
                                  StructField("statusId", IntegerType(), True),
                                  StructField("raceId", IntegerType(), True)])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json"
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - rename specific columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

results_df = results_df.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("resultId", "result_id")\
.withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
results_df = add_ingestion_date(results_df)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - drop unwanted rows

# COMMAND ----------

results_df = results_df.drop(col("statusId"))
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe the dataframe

# COMMAND ----------

# we are removing duplicated data
results_df = results_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1

# COMMAND ----------

# we are selecting distinct race_id and putting them in a list
# dropping the partition deletes both the data and the grouping because each partition is tied to a physical folder and deleting it deletes the entire group and its contents.
# race_id_list = results_df.select("race_id").distinct().collect()
# for race_id_list in list:
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2

# COMMAND ----------

#overwrite_partition(results_df, "f1_processed", "results", "race_id")

# COMMAND ----------

# from delta.tables import DeltaTable

# # check to see if the table exists
# if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#     # create a DeltaTable object for the target table
#     deltaTable = DeltaTable.forPath(spark, "abfss://process@formula1nesodatalake.dfs.core.windows.net/results")

#     # perform merge
#     deltaTable.alias("tgt").merge(
#         results_df.alias("src"),
#         "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
#     ).whenMatchedUpdateAll()\
#      .whenNotMatchedInsertAll()\
#      .execute()

# else:
#     # write for first-time load
#     results_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")


# COMMAND ----------

from delta.tables import DeltaTable
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"

# COMMAND ----------

from delta.tables import DeltaTable
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data("f1_processed", "results", results_df, "race_id", process_folder_path, merge_condition)

# COMMAND ----------

#spark.conf.set("spark.sql.sources.partitionOverwrite", "dynamic")

# COMMAND ----------

# results_df = results_df.select("result_id", "driver_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# #if the table exists then we just add the new data or else we create 
# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - write to parquet

# COMMAND ----------

#results_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC