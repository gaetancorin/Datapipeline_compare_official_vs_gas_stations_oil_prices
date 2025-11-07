# project_master_etl FLASK APIs
This Flask repository is an ETL pipeline to extract, transform, and load oil prices from gas stations and official government reports, then format the data and make it available for comparison to extract business KPIs.

**The Postman API import is available in the root folder.**

## **GET /is_alive **
This API allows to check the flask server

### **Params**
| Request Params | Field Type | Field Description| Optionnal Fill |
|---|---|---|---|


### **Response**
```json
"alive"
```

### **Example**
http://127.0.0.1:5000/is_alive


## **POST /etl/launch_complete_pipeline_oil_prices **
This API allows to launch complete pipeline for load all datas to mongo

### **Params**
| Request Params | Field Type | Field Description                                                         | Optionnal Fill                               |
|----------------|------------|---------------------------------------------------------------------------|----------------------------------------------|
| year_to_load     | string     | fill with 'yyyy' year you want to load                                    | Yes (load all dates if not fill)             |
| drop_mongo_collections     | string     | fill with "true" if you want trop all collections before starting loading | Yes (not drop if not fill) |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/etl/launch_complete_pipeline_oil_prices


## **POST /etl/launch_etl_gas_stations_oil_prices **
This API allows to launch part of pipeline for load specific gas_stations_oil_prices datas to mongo

### **Params**
| Request Params | Field Type | Field Description                                                                         | Optionnal Fill                               |
|----------------|------------|-------------------------------------------------------------------------------------------|----------------------------------------------|
| year_to_load     | string     | fill with 'yyyy' year you want to load                                                    | Yes (load all dates if not fill)             |
| drop_mongo_collections     | string     | fill with "true" if you want trop specific collection you work on before starting loading | Yes (not drop if not fill) |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/etl/launch_etl_gas_stations_oil_prices


## **POST /etl/launch_etl_official_oils_prices **
This API allows to launch part of pipeline for load specific official_oils_prices datas to mongo

### **Params**
| Request Params | Field Type | Field Description                                                                         | Optionnal Fill                               |
|----------------|------------|-------------------------------------------------------------------------------------------|----------------------------------------------|
| year_to_load     | string     | fill with 'yyyy' year you want to load                                                    | Yes (load all dates if not fill)             |
| drop_mongo_collections     | string     | fill with "true" if you want trop specific collection you work on before starting loading | Yes (not drop if not fill) |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/etl/launch_etl_official_oils_prices


## **POST /dataviz/denormalize_station_prices_for_dataviz **
This API allows to launch part of pipeline for get datas on Mongo, transform data into denormalize table, and load it in Mongo into another collection

### **Params**
| Request Params | Field Type | Field Description                                                                         | Optionnal Fill                                  |
|----------------|------------|-------------------------------------------------------------------------------------------|-------------------------------------------------|
| year_to_load     | string     | fill with 'yyyy' year you want to transform                                               | Yes (transform all dates available if not fill) |
| drop_mongo_collections     | string     | fill with "true" if you want trop specific collection you work on before starting loading | Yes (not drop if not fill)                      |


### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/dataviz/denormalize_station_prices_for_dataviz


## **POST /dataviz/merge_denorm_station_vs_official_prices **
This API allows to launch part of pipeline for get 'denormalize_station_prices' datas on Mongo, for get 'official_oils_prices' on Mongo, and merge the 2 tables at the same date for simplify futurs datavisualisations. After that, load this new table in Mongo into another collection

### **Params**
| Request Params | Field Type | Field Description                                                                         | Optionnal Fill                                  |
|----------------|------------|-------------------------------------------------------------------------------------------|-------------------------------------------------|
| year_to_load     | string     | fill with 'yyyy' year you want to transform                                               | Yes (transform all dates available if not fill) |
| drop_mongo_collections     | string     | fill with "true" if you want trop specific collection you work on before starting loading | Yes (not drop if not fill)                      |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/dataviz/merge_denorm_station_vs_official_prices


## **GET /mongo/list_all_mongo_collections **
This API allows to list all mongo databases name and collections name inside MongoDB.

### **Params**
| Request Params | Field Type | Field Description                          | Optionnal Fill            |
|----------------|------------|--------------------------------------------|---------------------------|

### **Response**
```json
{
  "datalake": [
    "gas_stations_infos",
    "gas_stations_price_logs_eur",
    "official_oils_prices"
  ],
  "denormalization": [
    "denorm_station_prices",
    "denorm_station_vs_official_prices"
  ]
}
```

### **Example**
http://127.0.0.1:5000/mongo/list_all_mongo_collections


## **POST /mongo/drop_one_collection **
This API allows to remove one collection on MongoDB and delete all datas inside of this collection.

### **Params**
| Request Params | Field Type | Field Description                                                                                         | Optionnal Fill            |
|----------------|------------|-----------------------------------------------------------------------------------------------------------|---------------------------|
| db_name     | string     | fill with db_name name where the collection you want to delete is                                         | No                        |
| collection_name     | string     | fill with collection name if you want delete | No |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/mongo/drop_one_collection


## **POST /mongo/drop_one_bdd **
This API allows to remove selected database on MongoDB and delete all collections and datas inside of this database (not removing all databases inside DBMS).

### **Params**
| Request Params | Field Type | Field Description                          | Optionnal Fill            |
|----------------|------------|--------------------------------------------|---------------------------|
| db_name     | string     | fill with database name you want to delete | No                        |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/mongo/drop_one_bdd


## **GET /utils/list_S3_contents **
This API allows to see all files and contents inside S3 (mongo_dump).

### **Params**
| Request Params | Field Type | Field Description                          | Optionnal Fill            |
|----------------|------------|--------------------------------------------|---------------------------|

### **Response**
```json
[
    {
        "Key": "datalake_2025_06_22.zip",
        "Size": 20902058
    },
    {
        "Key": "denormalization_2025_06_22.zip",
        "Size": 49649
    },
    {
        "Key": "test.txt",
        "Size": 18
    }
]
```

### **Example**
http://127.0.0.1:5000/utils/list_S3_contents


## **POST /utils/save_mongo_dump_to_S3 **
this API allows you to create a dump of a MongoDB database and automatically upload it to S3.
(If no database is specified, all relevant databases will be dumped by default.)

### **Params**
| Request Params | Field Type | Field Description                              | Optionnal Fill                                |
|----------------|------------|------------------------------------------------|-----------------------------------------------|
| db_name     | string     | fill with database name you want to dump to S3 | Yes (dump all relevant databases if not full) |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/utils/save_mongo_dump_to_S3


## **POST /utils/restore_mongo_dump_from_S3 **
This API allows you to get specific mongo dump from S3, and load it into Mongo by naming it the database as you want.

### **Params**
| Request Params | Field Type | Field Description                                                                     | Optionnal Fill |
|----------------|------------|---------------------------------------------------------------------------------------|----------------|
| zip_name     | string     | fill with 'name.zip' you found with API '/utils/list_S3_contents'                     | No             |
| new_bdd_name     | string     | fill with database name you want to give to your new database created by restore dump | No             |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5000/utils/restore_mongo_dump_from_S3