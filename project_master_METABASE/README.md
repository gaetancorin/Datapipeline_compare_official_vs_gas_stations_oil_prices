# Project_master_METABASE

## ✅ Prerequisites
Install Docker https://docs.docker.com/get-docker/

## 📥 Clone the Repository
```
git clone https://github.com/gaetancorin/Datapipeline_project_master.git
cd Datapipeline_project_master/Projet_master_RNCP/project_master_METABASE
```


## ⚙️ Setup
Create a .env file inside the env/ folder based on the env/.env_example file, and fill in the required variables.

## 🚀 Run the Project
Go to the root of the Project Project_master_METABASE and
Build and start the Docker-compose
```
docker-compose -p project_metabase_tools up --build -d
```
This will:
- Build and Start the Flask API project into container
- Build and Start the Metabase into container

### 📮 (Recommended) Import API Collection into Postman
For easier testing, import the '**Project_master_METABASE.postman_collection.json**' file into your local Postman application.
This will give you a ready-to-use set of API requests.


### 📮 (Recommended) Direct Import of metabase.db from Repository
For easier testing and faster setup, a sample metabase.db file is included directly in the Flask repository.

To use it:

1. Send a POST request to the endpoint:
**/utils/restore_metabase_db_from_S3**
using Postman or any API client.

2. In the request body, provide the following parameter value:
```
"zipname": "metabase_db_example"
```
3. Once the database is restored, open Metabase at:
http://localhost:3000
(user/password: admin@admin.com ; admin31)


4. (recommanded) Follow specific Metabase tutorial for our application:
https://open-documentations.s3.eu-west-3.amazonaws.com/Support+de+formation+Metabase.pdf