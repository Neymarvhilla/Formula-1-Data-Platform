# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

#  %sql
#  DROP TABLE IF EXISTS f1_presentation.calculated_race_results

# COMMAND ----------

spark.sql( f"""
    CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
    (
        race_year INT,
        team_name STRING,
        driver_id INT,
        driver_name STRING,
        race_id INT,
        position INT,
        points INT,
        calculated_points INT,
        created_date TIMESTAMP,
        updated_date TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql("USE f1_processed")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW race_result_updated
AS
SELECT races.race_year, constructors.name AS team_name, drivers.driver_id, drivers.name AS driver_name, races.race_id, results.position, results.points, 11 - results.position AS calculated_points
FROM results 
JOIN drivers  USING (driver_id)
JOIN constructors  USING (constructor_id)
JOIN races  USING (race_id)
WHERE results.position <= 10 AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

# note: you can wrap your sql code in your python notebooks using spark.sql()

# COMMAND ----------

#  %sql
#  SELECT * FROM race_result_updated

# COMMAND ----------

# %sql
# SELECT COUNT(1) FROM race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_result_updated upd
# MAGIC ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.position = upd.position,
# MAGIC              tgt.calculated_points = upd.calculated_points,
# MAGIC              tgt.updated_Date = current_timestamp
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_Date)
# MAGIC     VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)

# COMMAND ----------

# %sql
# CREATE TABLE f1_presentation.calculated_race_results
# USING parquet
# AS
# SELECT races.race_year, constructors.name AS team_name, drivers.name AS driver_name, results.position, results.points, 11 - results.position AS calculated_points
# FROM results 
# JOIN drivers  USING (driver_id)
# JOIN constructors  USING (constructor_id)
# JOIN races  USING (race_id)
# WHERE results.position <= 10

# COMMAND ----------

# %sql
# SELECT *
# FROM f1_presentation.calculated_race_results