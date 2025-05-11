-- Databricks notebook source
-- any table we create in this databse would be created/stored under the location path
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://process@formula1nesodatalake.dfs.core.windows.net/"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

