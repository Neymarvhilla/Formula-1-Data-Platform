-- Databricks notebook source
-- MAGIC %python
-- MAGIC html =
-- MAGIC displayHTML()

-- COMMAND ----------

USE f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, driver_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY driver_name, race_year
ORDER BY race_year, avg_points DESC