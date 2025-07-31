# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Read data
# MAGIC ### 2. Transform data
# MAGIC ### 3. Write data
# MAGIC ### 4. Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark data

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

dbutils.widgets.text("p_new_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_new_file_date")

# COMMAND ----------

# let's import data types we are interested in
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# we are reading the csv file with spark.
# we want spark to know that the first row represents column names.
# we want spark to be able to detect any numbers in the csv file.
circuits_df = spark.read.option("header", "true").schema(circuits_schema).csv(
    f"{raw_folder_path}/{v_file_date}/circuits.csv"
)

circuits_df.show()
circuits_df.printSchema()



# COMMAND ----------

# to see the type of circuits_df
type(circuits_df)

# COMMAND ----------

# to quickly view the dataframe, say the first 5 records
circuits_df.show(5)


# COMMAND ----------

# alternatively
display(circuits_df)

# COMMAND ----------

# to view the schema in our dataframe
circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only the required columns

# COMMAND ----------

# we want select specific columns from our dataframe and then saving it as a new dataframe.
circuits_select_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

display(circuits_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write data to datalake as parquet file

# COMMAND ----------

# use overwrite so we can rerun this notebook
#circuits_final_df.write.mode("overwrite").parquet(f"{process_folder_path}/circuits")

# COMMAND ----------

# creates a table called circuits and registers it under the f1_processed database and it will write the data to the folder path we have specified in our database
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls abfss://process@formula1nesodatalake.dfs.core.windows.net/circuits

# COMMAND ----------

dbutils.notebook.exit("Success")