-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, team_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY team_name, race_year
ORDER BY race_year, avg_points DESC