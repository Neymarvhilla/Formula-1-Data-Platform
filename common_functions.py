# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def spark_configuratio():
    return 

# COMMAND ----------

# We are creating a list of the distinct race_years
# race_results_list = spark.read.parquet(f"{presentation_folder_path}/final_results_report").filter(f"file_date = '{v_file_date}'").select("race_year").distinct().collect()

from pyspark.sql.functions import col

def create_list(input_table, column_name, file_date):
    df = spark.read.format("delta").load(f"{presentation_folder_path}/{input_table}")
    filtered = df.filter(col("file_date") == file_date)
    race_results_list = filtered.select(column_name).distinct().collect()
    return race_results_list


# COMMAND ----------

def rearrange_partition_column(input_df, partition_column):
    # we create an empty list
    column_list = []
    # loop through the the column names of the input dataframe
    for column_name in input_df.schema.names:
        #if the column name doesnt match the passed in partition column add it to the list
        if column_name != partition_column:
            column_list.append(column_name)
    # now we can add the partition column to our list ensuring it is the last element
    column_list.append(partition_column)
    print(column_list)
    output_df = input_df.select(column_list)
    return output_df       



# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = rearrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # check if the table exists
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        # insert the new data to the table
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        # create the table
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_data(db_name, dbt_name, input_df, partition_column, folder_path, merge_condition):
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{dbt_name}"):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{dbt_name}")
        deltaTable.alias("tgt").merge(
        input_df.alias("src"), merge_condition
    ).whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()

    else:
        # write for first-time load
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{dbt_name}")
        