# Project_master_ETL

## ✅ Prerequisites
Install Docker https://docs.docker.com/get-docker/

## 📥 Clone the Repository
```
git clone https://github.com/gaetancorin/Datapipeline_project_master.git
cd Datapipeline_project_master/Projet_master_RNCP/project_master_ETL
```


## ⚙️ Setup
Create a .env file inside the env/ folder based on the env/.env_example file, and fill in the required variables.

## 🚀 Run the Project
Go to the root of the Project Project_master_ETL and
Build and Start the Dockerfile:
```
docker build -t project_master_etl .
```
Start the container
```
docker run -d -p 5000:5000 --name project_master_etl --memory="15g" project_master_etl
```

### 📮 (Recommended) Import API Collection into Postman
For easier testing, import the '**Project_master_ETL.postman_collection.json**' file into your local Postman application.
This will give you a ready-to-use set of API requests.

### 📮 (Recommended) Check the technical documentation for a better understanding of the ETL and data visualization
https://open-documentations.s3.eu-west-3.amazonaws.com/Documentation+technique+Datapipeline_comparaison.pdf
