
# Datapipeline Compare Official vs Gas Stations Oil Prices

[<img src="https://img.shields.io/badge/LinkedIn-Gaetan%20Corin%20–%20Data%20Engineer-blue.svg?logo=linkedin" alt="LinkedIn" style="vertical-align: middle;"/>](https://www.linkedin.com/in/gaetancorin/)

This project processes **56 million records** of **oils prices changes** from French gas stations (2008 → today), and compares them with official government oil prices to analyze pricing differences.

### Tech Stack
- **Python** (ETL, data cleaning, automation)  
- **Flask** (API & scheduling)  
- **MongoDB** (storage & denormalization)  
- **Metabase** (visualization & dashboards)  
- **Amazon S3** (backup & restoration)  
- **Pandas** (data transformation & aggregation)  
- **Selenium** (web scraping and automation for government data)

## Pipelines and Visualizations
A **daily Flask server** orchestrates two ETL pipelines feeding **MongoDB**:

### Gas Station Data Pipeline
- Extraction, cleaning, anomaly filtering (**standard deviation + Z-score**), and structuring into two collections (`stations_infos` and `stations_prices`).  
- Data is then **denormalized** into a **daily average price per oil type** across all stations to match the format of the official data.  
- Results are loaded into **MongoDB**.

### Official Data Pipeline
- An **automated bot fills the government form** to generate the URL for downloading the CSV (the URL changes daily).  
- Extract the CSV data using the URL, then **transform and load it into MongoDB** (data is already denormalized by the government into **weekly averages** per oil type).

## Graph Analysis
- Data is analyzed in **Metabase**, which compares **observed daily oil prices** (gas stations data) with **official weekly oil prices** (official government data) to visualize differences.
- Additional analysis identifies **daily price patterns in gas station data**, which are not visible in the weekly government data.

## Infrastructure & Backup
Two **Flask servers** manage infrastructure tasks with **S3 storage**:

- **`project_ETL` server**: Handles ETL processing as well as **MongoDB dumps and restoration** to Amazon S3 using APIs.  
- **`project_METABASE` server**: Handles **Metabase dashboard backup and redeployment** to S3 via the API, ensuring all dashboards (charts, reports) are safely stored and restorable.
