# Football Data Engineering Project
# 1. Project Overview
This project implements a complete data engineering pipeline using football data.
The objective is to cover the full data lifecycle, from raw data ingestion to analytical
ta 

# 2. Datasets
The pipeline uses two related datasets:

- *Football matches dataset*  
  Contains information at match level, such as teams, goals, shots, expected goals (xG),
  possession, attendance, stadium, referee, league and season.

- *Football player statistics dataset* 
  Contains player performance data per match, including minutes played, goals, assists,
  passes, tackles, interceptions, cards and ratings.

Both datasets are linked using the match_id field.


# 3. Data Architecture
The project follows a layered architecture inspired by the medallion approach:

Bronze Layer
- Raw CSV files.
- Data is stored without modifications.
- Acts as the source of truth.

Silver Layer
- Data is cleaned and normalized in memory using pandas.
- Column names are standardized.
- Dates and numeric fields are parsed.
- Basic data quality rules are applied.

Gold Layer
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

The pipeline is designed to run as a daily batch process.


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
- *dim_team*: football teams
- *dim_player*: players (using player_id as natural key)
- *dim_date*: calendar attributes derived from match dates

# Fact Tables
- *fact_match*: match-level information and results
- *fact_player_match*: player performance per match

This model supports analytical queries.


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


# 8. Monitoring and Logging
Basic monitoring is implemented using Python logging.

- Each pipeline execution logs start and end messages.
- Logs are written to a file (pipeline.log) and to the console.
- Errors stop the pipeline execution.


# 9. Data Consumption
The data can be consumed by analytical users using SQL queries.
An example script (query_demo.py) shows how to query KPIs and business views.


# 10. How to Run the Project
Install dependencies:
pip install pandas duckdb

Run the code:
python src/pipeline.py

Show the results:
python src/query_demo.py


# 11. Conclusion
This project shows how a complete data engineering pipeline can be built from start to end using simple and clear tools. Raw football data is 
ingested, cleaned, validated and transformed into an analytical data warehouse that can be easily queried. The project covers the full data 
lifecycle, including data quality checks, data modeling, KPI generation and business views. The pipeline is modular, easy to understand and 
designed to run as a daily batch process, which makes it realistic and close to real data engineering work. Logging and basic monitoring are 
included to control pipeline executions and detect errors. Even though the project runs locally, it is designed in a way that allows future 
improvements, such as better orchestration, cloud storage or more advanced analytics. Overall, this project demonstrates a solid understanding 
of the main concepts of data engineering and how they can be applied to transform raw data into useful insights.

----------------------------------------------------------------------------------------------------------------------------------------------

# The scalability of the project should be considered, so you need to infer the performance and costs of multiplying the amount of raw data by
# x10, x100, x1000, x10^6 (That is, the number of rows). A proposal should be made to address those problems.
If the volume of raw data increases, the current solution may face performance limitations, mainly due to in-memory processing with pandas and
local execution.

- x10: The current solution would still work with small adjustments.
- x100: Processing time would increase noticeably; memory usage could become an issue.
- x1000: Pandas would no longer be efficient. Distributed processing would be required.
- x10⁶: A local solution would not be feasible.

Proposed solution:
- Store raw data in cloud object storage.
- Replace pandas with distributed tools (e.g. Spark).
- Use a cloud data warehouse for analytical queries.
- Use a workflow orchestrator to manage execution and retries.


# How much money will it cost if we migrate this to a cloud provider? Consider the x10, x100, x1000, x10^6 scenarios. Take one cloud provider
# to calculate it.
Assuming migration to AWS:

- *Storage (S3)*: very low cost per GB.
- *Processing (EC2 / Spark)*: cost increases with data volume and execution time.
- *Data warehouse (Athena / Redshift)*: cost depends on query usage.

Approximate estimation:
- x10: low monthly cost (few euros).
- x100: moderate cost (tens of euros).
- x1000: high cost (hundreds of euros).
- x10⁶: enterprise-level cost (thousands of euros).

Exact cost depends on usage patterns and optimization.


# Think about who can consume the data and design a solution to deliver it.
The data can be consumed by:
- Data analysts using SQL.
- BI dashboards (e.g. Looker Studio).
- Data scientists for further analysis or modeling.

The delivery solution would be:
- A shared analytical database.
- Read-only access for consumers.
- Predefined views for business users.


# Explain how AI can help with any of the processes.
AI can support several parts of the pipeline:
- Automatic data quality anomaly detection.
- Schema change detection.
- Query optimization suggestions.
- Automated insight generation from KPIs.
- Forecasting and predictive analytics based on historical data.

AI can improve efficiency and reduce manual work.


# Is there any concern about privacy?
The datasets contain no personal or sensitive information. Only sports performance data is processed, so privacy and GDPR risks are minimal.
However, access control would still be required in a real production environment.