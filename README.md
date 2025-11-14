
# Datapipeline Compare Official vs Gas Stations Oil Prices

[<img src="https://img.shields.io/badge/LinkedIn-Gaetan%20Corin%20–%20Data%20Engineer-blue.svg?logo=linkedin" alt="LinkedIn" style="vertical-align: middle;"/>](https://www.linkedin.com/in/gaetancorin/)

This project processes 56 million records of oils price changes from French gas stations (2008 → today).
A Flask server, running daily, orchestrates two ETL pipelines feeding MongoDB:

**Gas Station Data:** Extraction, cleaning, anomaly filtering (standard deviation + Z-score), and structuring into two collections (stations_infos and stations_prices).
The dataset is then denormalized into a daily average price per oil type across all stations to match the format of the official data. Results are loaded into MongoDB.

**Official data:** Extraction of government-provided oils price data, already preprocessed and denormalized into weekly averages per oil type, then directly loaded into MongoDB.

The data is analyzed in Metabase, which compares observed daily prices with official weekly prices to visualize differences.

Two Flask servers manage infrastructure tasks with S3 storage:

- The 'project_ETL' server handles ETL processing as well as MongoDB dumps and restoration to Amazon S3 using APIs.

- The 'project_METABASE' server handles Metabase dashboard backup and redeployment to S3 via the API, ensuring that all dashboards (charts, reports) are safely stored and restorable.

----
This project processes **56 million records** of **fuel price changes** from French gas stations (2008 → today).  
A **daily Flask server** orchestrates two ETL pipelines feeding **MongoDB**:

### Gas Station Data
- Extraction, cleaning, anomaly filtering (**standard deviation + Z-score**), and structuring into two collections (`stations_infos` and `stations_prices`).  
- Data is then **denormalized** into a **daily average price per fuel type** across all stations to match the format of the official data.  
- Results are loaded into **MongoDB**.

### Official Data
- Extraction of **government-provided fuel price data**, already preprocessed and denormalized into **weekly averages** per fuel type.  
- Data is directly loaded into **MongoDB**.

### Analysis
- Data is analyzed in **Metabase**, which compares **observed daily prices** with **official weekly prices** to visualize differences.

### Infrastructure & Backup
Two **Flask servers** manage infrastructure tasks with **S3 storage**:

- **`project_ETL` server**: Handles ETL processing as well as **MongoDB dumps and restoration** to Amazon S3 using APIs.  
- **`project_METABASE` server**: Handles **Metabase dashboard backup and redeployment** to S3 via the API, ensuring that all dashboards (charts, reports) are safely stored and restorable.