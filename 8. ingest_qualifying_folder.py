# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_new_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_new_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyingId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                     ]) 

# COMMAND ----------

# qualifying_df = spark.read.schema(qualifying_schema).option("multiline", "true").json(
#     f"{raw_folder_path}/qualifying/"
# )
qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingetsion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("qualifyingId", "qualifying_id").withColumnRenamed("constructorId", "constructor_id").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

qualifying_df = add_ingestion_date(qualifying_df)
display(qualifying_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Wrire to output to processed container in parquet format

# COMMAND ----------

#overwrite_partition(qualifying_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------



# COMMAND ----------

from delta.tables import DeltaTable
merge_condition = "tgt.qualifying_id = src.qualifying_id AND tgt.race_id = src.race_id"
merge_delta_data("f1_processed", "qualifying", qualifying_df, "race_id", process_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# display(spark.read.parquet(f"{process_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id