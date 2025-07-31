-- Databricks notebook source
SELECT *
FROM f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT driver_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT driver_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year Between 2011 AND 2020
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT driver_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year Between 2001 AND 2010
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC