# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file
# MAGIC

# COMMAND ----------

# lets import the types we need
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

# COMMAND ----------

# defining our schema
name_schema = StructType(fields = [StructField("forename", StringType(), True), StructField("surname", StringType(), True)])

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_new_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_new_file_date")

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False), StructField("driverRef", StringType(), True), StructField("number", IntegerType(), True), StructField("code", StringType(), True), StructField("name", name_schema), StructField("dob", DateType(), True), StructField("nationality", StringType(), True), StructField("url", StringType(), True)])

# COMMAND ----------

# read the file
drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columms and add new columns
# MAGIC 1. driverid renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("driverRef", "driver_ref") \
                                   .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                   .withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
drivers_with_columns_df = add_ingestion_date(drivers_with_columns_df)
display(drivers_with_columns_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

# dropping unwanted columns
drivers_final_df = drivers_with_columns_df.drop(col("url"))
display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# write to parquet
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")