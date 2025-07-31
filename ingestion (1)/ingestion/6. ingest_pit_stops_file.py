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
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline", "true").json(
#     f"{raw_folder_path}/pit_stops.json"
# )
pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingetsion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

pit_stops_df = pit_stops_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
pit_stops_df = add_ingestion_date(pit_stops_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Wrire to output to processed container in parquet format

# COMMAND ----------

# without delta tables
#overwrite_partition(pit_stops_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

from delta.tables import DeltaTable
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data("f1_processed", "pit_stops", pit_stops_df, "race_id", process_folder_path, merge_condition)

# COMMAND ----------

# pit_stops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# display(spark.read.parquet(f"{process_folder_path}/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")