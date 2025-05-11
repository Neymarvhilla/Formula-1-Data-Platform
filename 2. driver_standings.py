# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find race years for which the data is to be reprocessed

# COMMAND ----------

# We are creating a list of the distinct race_years
# race_results_list = spark.read.parquet(f"{presentation_folder_path}/final_results_report").filter(f"file_date = '{v_file_date}'").select("race_year").distinct().collect()

# COMMAND ----------

race_results_list = create_list("race_results", "race_year", v_file_date)

# COMMAND ----------

race_results_list

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

#print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col
#race_results_df = spark.read.parquet(f"{presentation_folder_path}/final_results_report").filter(col("race_year").isin(race_year_list))

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year","driver_name", "driver_nationality").agg(sum("points").alias("total_points"), count(when(race_results_df["position"] == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

#overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

from delta.tables import DeltaTable
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data("f1_presentation", "driver_standings", final_df, "race_year", presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.driver_standings