# Databricks notebook source
v_result = dbutils.notebook.run("1. ingest_circuits_file", 0, {"p_data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2. ingest_races_file", 0, {"p_data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. ingest_constructors_file", 0, {"p_data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. ingest_drivers_file", 0, {"p_data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. ingest_results_file", 0, {"p_data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. ingest_pit_stops_file", 0, {"p_data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7. ingest_lap_times_folder", 0, {"p_data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8. ingest_qualifying_folder", 0, {"p_data_source": "Ergast API", "file_date": "2021-03-21"})

# COMMAND ----------

v_result