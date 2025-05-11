# 🏎️ Formula 1 Data Engineering Project

This project demonstrates a complete **cloud-based data engineering pipeline** using **Azure Databricks**, **Azure Data Lake Storage (ADLS Gen2)**, and **Azure Data Factory (ADF)**. It involves ingesting, transforming, and serving structured Formula 1 data for analytics and reporting.

---

## 🚀 Technologies Used

- **Azure Data Lake Storage Gen2 (ADLS)** – For storing raw, processed, and curated data
- **Azure Databricks** – For all data processing using PySpark and Delta Lake
- **Delta Lake** – For versioned, ACID-compliant data lake tables
- **Azure Data Factory (ADF)** – For orchestration and automation
- **PySpark** – For data transformations
- **GitHub** – For version control

---

## 🧱 Project Structure

```bash
f1-data-engineering/
│
├── includes/
│   ├── configuration.py          # Global folder path configs
│   └── common_functions.py       # Utility functions like add_ingestion_date()
│
├── notebooks/
│   ├── circuits_ingestion.py
│   ├── races_ingestion.py
│   ├── results_merge_logic.py
│   ├── final_results_report.py
│   └── ...
│
├── resources/
│   ├── circuits.csv
│   ├── races.csv
│   ├── results.json
│   └── ...
│
├── pipelines/
│   └── orchestrator_adf.json     # ADF pipeline logic and metadata
│
└── README.md
```
Data Flow Architecture
Ingestion

Raw CSV and JSON files are stored in the raw layer of ADLS.

Data is read into Databricks with defined schemas for data quality.

Processing

Data is cleaned, transformed, and written to the processed layer in Delta format.

Incremental loads are handled using partition overwrite and merge strategies.

Presentation

Final transformed data is stored in the presentation layer for reporting.

External tables and Delta Lake tables are registered in Hive metastore.

Orchestration

ADF pipelines coordinate all steps: ingestion → transformation → reporting.

🧠 Key Concepts Demonstrated
Schema enforcement using StructType

Partitioning strategies for scalable writes

MERGE INTO statements for incremental data loads

Delta Lake tables and table versioning

Parameterization via dbutils.widgets

Use of temporary views and SQL queries in Databricks

Orchestration with Azure Data Factory linked services and pipelines

Use of the file_date parameter for time-based filtering

📈 Sample Use Case
Load race results incrementally each race week

Merge new results into existing Delta tables without duplicates

Join across circuits, drivers, results, and constructors

Generate a final report and publish it for Power BI or downstream apps

📂 Paths Used
Raw Layer: abfss://raw@formula1nesodatalake.dfs.core.windows.net/

Processed Layer: abfss://process@formula1nesodatalake.dfs.core.windows.net/

Presentation Layer: abfss://presentation@formula1nesodatalake.dfs.core.windows.net/

🧪 How to Run
Spin up a Databricks cluster with Spark 3.x runtime.

Upload notebooks under /notebooks and run sequentially.

Set up ADF pipeline triggers to automate weekly ingestion.

Run your SQL-based reports or feed the curated layer into Power BI.

📌 Author Notes
This project was built as part of my hands-on learning journey into cloud data engineering, leveraging project-based learning to build job-ready skills.
