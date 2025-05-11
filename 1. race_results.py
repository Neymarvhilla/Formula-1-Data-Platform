# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# lets read in all the necessary dataframes required 
#circuits_df = spark.read.parquet(f"{process_folder_path}/circuits").withColumnRenamed("name", "circuit_name").withColumnRenamed("location", "circuit_location")
circuits_df = spark.read.format("delta").load(f"{process_folder_path}/circuits").withColumnRenamed("name", "circuit_name").withColumnRenamed("location", "circuit_location")
#display(circuits_df)
#races_df = spark.read.parquet(f"{process_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("date", "race_date").withColumnRenamed("time", "race_time")
races_df = spark.read.format("delta").load(f"{process_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("date", "race_date").withColumnRenamed("time", "race_time")
#display(races_df)
#drivers_df = spark.read.parquet(f"{process_folder_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("nationality", "driver_nationality").withColumnRenamed("number", "driver_number")
drivers_df = spark.read.format("delta").load(f"{process_folder_path}/drivers").withColumnRenamed("number", "driver_number").withColumnRenamed("name", "driver_name").withColumnRenamed("nationality", "driver_nationality")
#display(drivers_df)
# we need the file_date to match with file date being passed in our widget
results_df = spark.read.format("delta").load(f"{process_folder_path}/results").filter(f"file_date = '{v_file_date}'").withColumnRenamed("time", "results_race_time").withColumnRenamed("race_id", "results_race_id").withColumnRenamed("file_date", "results_file_date")
#display(results_df)
#constructors_df = spark.read.parquet(f"{process_folder_path}/constructors").withColumnRenamed("name", "team").withColumnRenamed("nationality", "constructor_nationality")
constructors_df = spark.read.format("delta").load(f"{process_folder_path}/constructors").withColumnRenamed("name", "team").withColumnRenamed("nationality", "constructor_nationality")
#display(constructors_df)


# COMMAND ----------

results_report_df = circuits_df.join(races_df, circuits_df["circuit_id"] == races_df["circuit_id"], "inner")\
                                                .join(results_df, races_df["race_id"] == results_df["results_race_id"], "inner")\
                                                .join(drivers_df, results_df["driver_id"] == drivers_df["driver_id"], "inner")\
                                                .join(constructors_df, results_df["constructor_id"] == constructors_df["constructor_id"], "inner")

# COMMAND ----------

display(results_report_df)

# COMMAND ----------

results_report_df = results_report_df.select(results_report_df["results_file_date"],results_report_df["race_id"],results_report_df["race_year"], results_report_df["race_name"], results_report_df["race_date"], results_report_df["circuit_location"], results_report_df["driver_name"], results_report_df["driver_number"], results_report_df["driver_nationality"], results_report_df["team"], results_report_df["grid"], results_report_df["fastest_lap"], results_report_df["race_time"], results_report_df["points"], results_report_df["position"]).withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

results_report_df.printSchema()

# COMMAND ----------

display(results_report_df)

# COMMAND ----------

results_report_df.printSchema()

# COMMAND ----------

final_report_df = add_ingestion_date(results_report_df)

# COMMAND ----------

display(final_report_df)

# COMMAND ----------

final_report_df = final_report_df.withColumnRenamed("ingestion_date", "created_date")

# COMMAND ----------

display(final_report_df)

# COMMAND ----------

display(final_report_df.filter((final_report_df["race_year"] == 2020) & (final_report_df["race_name"] == "Abu Dhabi Grand Prix")))

# COMMAND ----------

#final_report_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.final_results_report")

# COMMAND ----------

#overwrite_partition(final_report_df, "f1_presentation", "final_results_report", "race_id" )

# COMMAND ----------

# with delta

from delta.tables import DeltaTable
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta_data("f1_presentation", "race_results", final_report_df, "race_id", presentation_folder_path, merge_condition)