# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingets constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_new_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_new_file_date")

# COMMAND ----------

# Defining our schema using DDL
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(
    f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted columns

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")
display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename specific columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
constructor_final_df = add_ingestion_date(constructor_final_df)
display(constructor_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to parquet

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")