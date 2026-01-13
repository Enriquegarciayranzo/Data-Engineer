# Football Data Engineering Project
# 1. Project Overview
This project implements a complete data engineering pipeline using football data.
The objective is to cover the full data lifecycle, from raw data ingestion to analytical tables that can be used for business insights.

The project has been developed as part of the Data Engineering course and follows
good practices in data architecture, data quality, orchestration and monitoring.


# 2. Datasets
The pipeline uses two related datasets:

- **Football matches dataset**  
  Contains information at match level, such as teams, goals, shots, expected goals (xG), possession, attendance, stadium, referee, league and season.

- **Football player statistics dataset**  
  Contains player performance data per match, including minutes played, goals, assists, passes, tackles, interceptions, cards and ratings.

Both datasets are linked using the match_id field.


# 3. Data Architecture
The project follows a layered architecture inspired by the medallion approach:

# Bronze Layer
- Raw CSV files.
- Data is stored without modifications.
- Acts as the source of truth.

# Silver Layer
- Data is cleaned and normalized in memory using pandas.
- Column names are standardized.
- Dates and numeric fields are parsed.
- Basic data quality rules are applied.

# Gold Layer
- Implemented using DuckDB.
- Contains analytical tables:
  - Dimension tables
  - Fact tables
  - KPIs
  - Business views

This separation improves data reliability and maintainability.


# 4. Pipeline Design and Orchestration
The pipeline is orchestrated from a central script (pipeline.py).

The execution flow is:
1. Extract raw CSV datasets  
2. Transform and clean data  
3. Apply data quality checks  
4. Load data into staging tables  
5. Build data warehouse tables  
6. Generate KPIs  
7. Create business views  

Each step is implemented in a separate module, which makes the pipeline easy to
understand, maintain and extend.

The pipeline is designed to run as a **daily batch process**.
In a production environment, it could be scheduled using:
- Windows Task Scheduler or cron
- GitHub Actions (scheduled workflow)
- A workflow orchestrator such as Airflow


# 5. Data Quality
Basic data quality validations are implemented, including:

- Required columns existence
- Non-empty datasets
- Unique and non-null match_id in matches
- Referential integrity between matches and player statistics
- Reasonable value ranges (e.g. possession percentage, minutes played)
- Correct date parsing

If any validation fails, the pipeline stops to avoid loading incorrect data.


# 6. Data Warehouse Model
The DuckDB data warehouse follows a star schema:

# Dimensions
- **dim_team**: football teams
- **dim_player**: players (using `player_id` as natural key)
- **dim_date**: calendar attributes derived from match dates

# Fact Tables
- **fact_match**: match-level information and results
- **fact_player_match**: player performance per match

This model supports analytical queries and BI tools.


# 7. KPIs and Business Views
Several KPIs are generated in the gold layer, such as:

- Average total goals per match
- Average attendance
- Average player rating
- Average minutes played

In addition, business-oriented views are created, including:

- Top players by rating
- Team goals vs expected goals (xG)
- League table based on points
- Match performance differentials
- Defensive intensity by team

These outputs show how raw data can be transformed into useful insights.


# 8. Monitoring and Logging
Basic monitoring is implemented using Python logging.

- Each pipeline execution logs start and end messages.
- Logs are written to a file (pipeline.log) and to the console.
- Errors stop the pipeline execution.

This allows simple monitoring and debugging.


# 9. Scalability Considerations
If data volume increases (x10, x100, x1000):

- Raw data could be stored in cloud object storage.
- Pandas transformations could be replaced by distributed processing tools.
- DuckDB could be migrated to a cloud data warehouse.
- A dedicated orchestrator could manage retries and alerts.

The modular design makes these changes possible with limited refactoring.


# 10. Data Consumption
The data can be consumed by:

- Data analysts using SQL
- BI dashboards
- Further analytical or machine learning tasks

An example script (query_demo.py) shows how to query KPIs and business views.


# 11. Privacy and Ethics
The datasets do not contain personal or sensitive data.
Only sports performance information is processed, so privacy risks are minimal.


# 12. How to Run the Project
Install dependencies:
pip install pandas duckdb

Run the code:
python src/pipeline.py

Show the results:
python src/query_demo.py


# 13. Conclusion
This project shows how a complete data engineering pipeline can be built from start to end using simple and clear tools. Raw football data is 
ingested, cleaned, validated and transformed into an analytical data warehouse that can be easily queried. The project covers the full data 
lifecycle, including data quality checks, data modeling, KPI generation and business views. The pipeline is modular, easy to understand and 
designed to run as a daily batch process, which makes it realistic and close to real data engineering work. Logging and basic monitoring are 
included to control pipeline executions and detect errors. Even though the project runs locally, it is designed in a way that allows future 
improvements, such as better orchestration, cloud storage or more advanced analytics. Overall, this project demonstrates a solid understanding 
of the main concepts of data engineering and how they can be applied to transform raw data into useful insights.