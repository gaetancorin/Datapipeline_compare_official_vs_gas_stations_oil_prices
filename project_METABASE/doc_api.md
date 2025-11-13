# project_master_METABASE FLASK APIs
This Flask repository is an APIs points for see S3 contents, extract Metabase db and save to S3, or get Metabase db saved from S3 and loading on Metabase.

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
http://127.0.0.1:5001/is_alive


## **GET /launch_metabase **
This API allows to launch metabase docker if exist

### **Params**
| Request Params | Field Type | Field Description| Optionnal Fill |
|---|---|---|---|


### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5001/launch_metabase

## **GET /stop_metabase **
This API allows to stop metabase docker if exist

### **Params**
| Request Params | Field Type | Field Description| Optionnal Fill |
|---|---|---|---|


### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5001/stop_metabase


## **GET /utils/list_S3_contents **
This API allows to see all files and contents inside S3 (Metabase_dump).

### **Params**
| Request Params | Field Type | Field Description                          | Optionnal Fill            |
|----------------|------------|--------------------------------------------|---------------------------|

### **Response**
```json
[
    {
        "Key": "metabase_db/metabase.db_2025_06_24.zip",
        "Size": 192967
    },
    {
        "Key": "metabase_db/metabase.db_2025_06_27.zip",
        "Size": 192967
    }
]
```

### **Example**
http://127.0.0.1:5001/utils/list_S3_contents


## **POST /utils/save_metabase_db_to_S3 **
this API allows you to create a save of a Metabase database locally and automatically upload it to S3.

### **Params**
| Request Params | Field Type | Field Description                              | Optionnal Fill                                |
|----------------|------------|------------------------------------------------|-----------------------------------------------|

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5001/utils/save_metabase_db_to_S3


## **POST /utils/restore_metabase_db_from_S3 **
This API allows you to get specific Metabase database from S3, and load it directly into Metabase.
You can load example metabase db by writing "metabase_db_example" into zip_name parameter.

### **Params**
| Request Params | Field Type | Field Description                                                             | Optionnal Fill |
|----------------|------------|-------------------------------------------------------------------------------|----------------|
| zip_name     | string     | fill with 'metabase_db/name.zip' you found with API '/utils/list_S3_contents' | No             |

### **Response**
```json
"done"
```

### **Example**
http://127.0.0.1:5001/utils/restore_metabase_db_from_S3