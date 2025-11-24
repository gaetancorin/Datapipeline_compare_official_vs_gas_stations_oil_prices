# Project_ETL

[<img src="https://img.shields.io/badge/LinkedIn-Gaetan%20Corin%20–%20Data%20Engineer-blue.svg?logo=linkedin" alt="LinkedIn" style="vertical-align: middle;"/>](https://www.linkedin.com/in/gaetancorin/)

This part of the project is responsible for performing all ETLs (Extract, Transform, Load) transformations.

## Prerequisites
* **MongoDB** installed and running
* [**MongoDB Command Line Database Tools**](https://www.mongodb.com/try/download/database-tools) installed
* **Docker** installed
* [**AWS S3 Account**](https://us-east-1.console.aws.amazon.com/s3/home?region=us-east-1#) and [**IAM User**](https://us-east-1.console.aws.amazon.com/iam/home#/users) with permissions:
    - s3:GetObject
    - s3:PutObject
    - s3:DeleteObject
    - s3:ListBucket

if running locally without Docker:
* **Google Chrome**  installed (required for Selenium / browser automation)


## Project Setup

If you haven't already cloned the repository:

```
git clone https://github.com/gaetancorin/Datapipeline_compare_official_vs_gas_stations_oil_prices.git
```
Then navigate to the ETL project folder:
```
cd Datapipeline_compare_official_vs_gas_stations_oil_prices/project_master_ETL
```

## Environment Setup
Provide the required environment variables for the project in the env/ folder.

### ➡️ Running project_ETL Locally (Without Docker) – Debugging Mode
1. Go to the `env` folder.
2. Copy `.env_example` to `.env` manually.
3. Fill in your **MongoDB credentials**, **Amazon IAM user**, and **S3 bucket information**.

This setup is useful for debugging or running the ETL directly on your machine.

### ➡️ Running project_ETL Locally (With Docker)
1. Go to the `env` folder.
2. Copy `.env_example` to `.env_prod` manually.
3. Fill in your **MongoDB credentials**, **Amazon IAM user**, and **S3 bucket information**.

This setup is intended for running the ETL inside a Docker container.


## Running the Project
From the `Project_ETL` root folder, build and start the Docker image:
```
docker build -t project_etl .
```
Run the container:
```
docker run -d -p 5000:5000 --name project_etl project_etl
```

## Recommended Tools & Documentation

### ➡️ API Resources
- **Postman Collection**  
  Import the Project_master_ETL.postman_collection.json file into Postman for a ready-to-use set of API requests.

- **API Documentation**  
  Detailed explanations, parameters, and usage examples of all ETL APIs and their endpoints:  
  [ETL API Documentation](doc_api.md)

### ➡️ Functions & Data Architecture
- **ETL Pipelines & MongoDB Structure**  
  Full architecture of the ETL functions and MongoDB collections:  
  [API and Functions Architecture](API_and_ETL_Architecture.md)

### ➡️ Technical Documentation
- **ETL & Data Visualization**  
  Comprehensive technical documentation for understanding the ETL process and dashboards:  
  [Technical Documentation PDF](https://open-documentations.s3.eu-west-3.amazonaws.com/Documentation+technique+Datapipeline_comparaison.pdf)