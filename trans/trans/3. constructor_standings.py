# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# We are creating a list of the distinct race_years
#race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/final_results_report").filter(f"file_date = '{v_file_date}'").select("race_year").distinct().collect()

# COMMAND ----------

race_results_list = create_list("race_results", "race_year", v_file_date)

# COMMAND ----------



# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

#print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col
constructor_standings = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(constructor_standings.filter(constructor_standings["race_year"] == 2020))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count

# COMMAND ----------

constructor_standings = constructor_standings.groupBy(constructor_standings["team"], constructor_standings["race_year"]).agg(count(when(constructor_standings["position"] == 1, True)).alias("wins"), sum(constructor_standings["points"]).alias("total_points"))

# COMMAND ----------

display(constructor_standings.filter(constructor_standings["race_year"] == 2020))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_constructor_rank = constructor_standings.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

# we are saving it as a table in our f1_presentation database with the name constructor_standings. this database is under the presentation layer in our adls
#final_constructor_rank.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

#overwrite_partition(final_constructor_rank, "f1_presentation", "constructor_standings", "race_year")

# COMMAND ----------

from delta.tables import DeltaTable
merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data("f1_presentation", "constructor_standings", final_constructor_rank, "race_year", presentation_folder_path, merge_condition)

# COMMAND ----------

#display(spark.read.parquet(f"{presentation_folder_path}/constructor_standings"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.constructor_standings