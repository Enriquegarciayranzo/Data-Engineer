**Football Data Project**
1. Project Overview
This project is designed to analyze football data in an organized and automated way.
The original data is collected, reviewed, cleaned, and combined before being stored in a well-structured database, designed to allow for useful conclusions and analyses.
The entire process runs automatically and can be easily repeated using Python.

2. Objective
The goal is to transform match and player statistics into clear and reliable data that can answer questions such as:
Which players perform best according to their ratings?
Which teams score more or fewer goals than expected based on their xG?
Which aspects of the game (xG, shots, possession, etc.) are related to winning a match?

3. Datasets
- futbol_matches.csv: Information for each match (teams, goals, xG, possession, assists, and result).
-futbol_player_stats.csv: Player statistics for each match.
The player identifiers in the original dataset were anonymized. For the presentation, players were searched for and inserted match by match to improve the presentation.

4. Architecture
The project uses a layered data architecture:
Bronze: Original, unprocessed CSV files.
Silver: Cleaned and normalized data after transformations.
Gold: Data warehouse in DuckDB with fact tables, dimensions, KPIs, and views for analysis.

5. ETL Pipeline Description
The pipeline (`pipeline.py`) does the following:

**Extract**
Reads CSV files and handles encoding issues.

**Transform**
Cleans column names and adjusts data types.
Processes dates and numeric values.
Unifies team names to avoid duplicates.

**Data Quality**
Checks that the primary key (`match_id`) is valid.
Checks consistency between tables.
Validates logical ranges (minutes played, possession percentages).

**Load**
Creates staging tables.
Builds a data warehouse with a star schema.

**Analysis**
Generates KPI tables.
Creates views for data analysis.

**Logging**
Records the status of each execution and any errors.

6. Data Warehouse Model
**Dimensions**
dim_team --> quién juega
dim_player --> quién rinde
dim_date --> cuándo ocurre

**Fact Tables**
fact_match --> Match-level data and results.
fact_player_match --> Player performance per match.

7. Business Insights
Analytical queries are exposed as database views, including:
- Top players by rating
- Teams: goals vs expected goals (xG)
- League table (points and goal difference)
- Match performance deltas (relative differences in xG, shots, possession)

8. How to Run the Project
.venv\Scripts\Activate.ps1 --> Activate virtual environment
python src/pipeline.py --> Run the pipeline
python src/run_queries.py --> Run business queries

9. Scalability of the project
Scenario     Rows        Expected impact               
  x10        ~10³      No performance issues         
  x100       ~10⁴      Slight slowdown               
  x1000      ~10⁵      Use Parquet and partitioning  
  x10⁶       ~10⁶      Cloud data warehouse required
**Use Parquet instead of CSV**
Parquet takes up less space and is read much faster, improving pipeline performance.

**Partition data by season or date**
This allows you to work with only a portion of the data (for example, a specific season) without loading the entire historical data.

**Incremental loading**
Instead of processing all the data each time, only new or updated matches are added, saving time and resources.

**Migration to cloud platforms**
Moving the project to the cloud makes it easier to scale, automate, and analyze large volumes of data with specialized tools.ç

10. Cloud Cost Estimation
Cloud provider considered: Google BigQuery.
- Storage: ~$0.02 per GB/month
- Query cost: ~$5 per TB scanned
Even with 1 million rows, estimated monthly cost remains low (a few dollars).

11. Data Consumption
Potential consumers:
- Data analysts (SQL queries).
- Data scientists (feature extraction).

Delivery mechanisms:
- DuckDB / BigQuery SQL access.

12. Role of AI
Detect errors in data --> AI can alert you to unusual or inconsistent values.
Detect changes in data --> If a file's structure suddenly changes, AI will detect it.
Automatically create documentation --> It can explain what each table and field is, eliminating the need for manual documentation.
Improve queries and generate insights --> It helps speed up queries and uncover interesting patterns in the data.

13. Privacy
- No personal identifiable information (PII) is used.
- Player data refers to professional performance.
- Names are used only for analytical clarity.

14. Conclusion
This work presents a comprehensive Data Engineering project, from raw data to actionable information for analysis and decision-making. It
demonstrates that Data Engineering is not just about storing information, but also about cleaning, validating, and structuring it correctly.
An ETL process was implemented to correct common errors and ensure data quality, preventing incorrect conclusions. Subsequently, the
information was integrated into a star schema Data Warehouse, facilitating analysis, avoiding duplication, and allowing the system to scale.
The layered architecture (Bronze, Silver, and Gold) clearly separates the original data, the cleaned data, and the final data for analysis,
improving maintenance and traceability.
From the Data Warehouse, relevant KPIs and analyses were generated, such as player performance and the relationship between xG and results
showing how well-prepared data generates clear insights.
Finally, aspects of scalability, cloud usage, GDPR and the support of Artificial Intelligence have been considered, concluding that the project
is solid, scalable and oriented towards obtaining real value from the data.