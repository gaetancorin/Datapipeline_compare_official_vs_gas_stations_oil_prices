import pymongo
import os
import pandas as pd
import subprocess
from dotenv import load_dotenv
from pymongo import ReplaceOne

load_dotenv('env/.env')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = int(os.getenv('MONGO_PORT'))
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
AUTH_DB = os.getenv('AUTH_DB')

# Setup MongoDB connection (local)
client_mongo = pymongo.MongoClient(
    host=MONGO_HOST,
    port=MONGO_PORT,
    username=MONGO_USER,
    password=MONGO_PASSWORD,
    authSource=AUTH_DB
)


def build_mongo_uri(db_name=None):
    # base_uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"
    base_uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
    if db_name:
        base_uri += f"/{db_name}?authSource={AUTH_DB}"
    else:
        base_uri += f"/?authSource={AUTH_DB}"
    return base_uri


def get_last_data_date_from_one_collection(db_name, collection):
    db_mongo = client_mongo.get_database(db_name)
    if collection not in db_mongo.list_collection_names():
        print(f"[INFO] Collection '{collection}' not exist in mongo BDD '{db_name}'")
        return None
    collection_mongo = db_mongo.get_collection(collection)
    # Research last date existing in collection
    latest_row = collection_mongo.find_one(
        {"Date": {"$exists": True}},
        sort=[("Date", -1)]
    )
    if latest_row and "Date" in latest_row:
        latest_date = latest_row["Date"]
        print(f"[INFO] Found last date '{latest_date}' into '{collection}' collection")
        return latest_date
    else:
        print(f"[INFO] Not found row with 'Date' inside '{collection}' collection")
        return None

def update_gas_stations_infos(gas_stations_infos, db_name, collection):
    db_mongo = client_mongo.get_database(db_name)
    collection_mongo = db_mongo.get_collection(collection)
    collection_mongo.create_index([("Id_station_essence", pymongo.ASCENDING), ("Cp", pymongo.ASCENDING)])
    records = gas_stations_infos.to_dict(orient="records")

    # Replace gas_station_infos row only if "Id_station_essence" matches and row's Last_update is more recent than in Mongo.
    operations = [
        ReplaceOne(
            {
                "Id_station_essence": record["Id_station_essence"],
                "$or": [
                    {"Derniere_maj": {"$lt": record["Derniere_maj"]}},
                    {"Derniere_maj": {"$exists": False}}
                ]
            },
            record,
            upsert=False
        )
        for record in records
    ]
    collection_mongo.bulk_write(operations)

    # If no matching "Id_station_essence exists", insert the row.
    existing_ids = set(doc["Id_station_essence"] for doc in collection_mongo.find({}, {"Id_station_essence": 1}))
    records_to_insert = [r for r in records if r["Id_station_essence"] not in existing_ids]
    if records_to_insert:
        collection_mongo.insert_many(records_to_insert)
    print("correctly update gas_stations_infos datas to MongoDB")


def load_datas_to_mongo(df, bdd, collection, index=None):
    db_mongo = client_mongo.get_database(bdd)
    collection_mongo = db_mongo.get_collection(collection)

    if index != None:
        formated_index = []
        for col in index:
            formated_index.append((col, pymongo.ASCENDING))
        collection_mongo.create_index(formated_index)

    records = df.to_dict(orient="records")
    collection_mongo.insert_many(records)
    return "done"


def get_filtered_datas_from_one_collection(start_date_to_load, end_date_to_load, db_name, collection):
    db_mongo = client_mongo.get_database(db_name)
    if collection not in db_mongo.list_collection_names():
        print(f"[INFO] Collection '{collection}' not exist in mongo BDD '{db_name}'")
        return None
    collection_mongo = db_mongo.get_collection(collection)
    cursor = collection_mongo.find(
        {
            "Date": {"$gte": start_date_to_load, "$lt": end_date_to_load}
        },{}
    )
    df = pd.DataFrame(list(cursor))
    return df


def does_database_exist(database_name):
    return database_name in client_mongo.list_database_names()


def does_collection_name_exist(database_name, collection_name):
    db_mongo = client_mongo.get_database(database_name)
    return collection_name in db_mongo.list_collection_names()


def drop_mongo_bdd(db_name):
    client_mongo.drop_database(db_name)


def drop_mongo_collections(bdd, collections):
    db_mongo = client_mongo.get_database(bdd)
    for collection in collections:
        print(f"[INFO] Into Mongo, drop '{collection.upper()}' collection in '{bdd.upper()}' bdd")
        db_mongo.drop_collection(collection)

def list_all_collections():
    result = {}
    databases = client_mongo.list_database_names()
    databases = [db for db in databases if db not in ["admin", "config", "local"]]
    for database in databases:
        db_mongo = client_mongo.get_database(database)
        collections = db_mongo.list_collection_names()
        for collection in collections:
            print(f"[INFO] Found {collection} collection in '{database}' database.")
            if database in result:
                result[database].append(collection)
            else:
                result[database] = [collection]
    return result


def mongodump(db_name, out_path):
    uri = build_mongo_uri(db_name)
    cmd = ['mongodump', '--uri', uri, '--out', out_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[INFO] Dump '{db_name.upper()}' database success in {out_path}")
    else:
        print("[ERROR]:", result.stderr)


def mongorestore(dump_path, old_db_name, new_db_name):
    print(f"[INFO] Starting restore old dump db '{old_db_name}' into new db '{new_db_name}'")
    uri = build_mongo_uri()
    cmd = [
        'mongorestore',
        '--uri', uri,
        f'--nsFrom={old_db_name}.*',
        f'--nsTo={new_db_name}.*',
        dump_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[INFO] Successfully restored old dump db '{old_db_name}' into new db '{new_db_name}'")
    else:
        print("[ERROR]:", result.stderr)
