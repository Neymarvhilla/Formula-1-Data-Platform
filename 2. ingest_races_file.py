# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_new_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_new_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# create out schemas manually
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", StringType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)])

# COMMAND ----------

# read the file using our schemas
races_df = spark.read.option("header", "True").schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# select only the columns we require
selected_races_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))
display(selected_races_df)

# COMMAND ----------

# performing transformations like renaming specific columns
renamed_races_df = selected_races_df.withColumnRenamed("raceId", "race_id") \
                                  .withColumnRenamed("year", "race_year") \
                                  .withColumnRenamed("circuitId", "circuit_id")\
                                  .withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

display(renamed_races_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
races_final_df = add_ingestion_date(renamed_races_df)
display(races_final_df)
from pyspark.sql.functions import concat_ws, to_timestamp

# Create a new column that combines date and time into a timestamp
races_final_df = races_final_df.withColumn(
    "race_timestamp",
    to_timestamp(concat_ws(' ', races_df.date, races_df.time), "yyyy-MM-dd HH:mm:ss")
)
display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")