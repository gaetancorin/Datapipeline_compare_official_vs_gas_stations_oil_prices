import pandas as pd
import os
import shutil
from datetime import datetime
import App.mongo_manager as mongo_manager
import App.S3_manager as S3_manager

def determine_dates_to_load_from_mongo(year_to_load, db_name, collection):
    if year_to_load:
        # Load only the target year
        print(f"[INFO] year_to_load received: {year_to_load}")
        start_date_to_load = pd.to_datetime(f"{year_to_load}-01-01")
        end_date_to_load = pd.to_datetime(f"{year_to_load}-12-31")
    else:
        print("[INFO] No year_to_load provided, so load only new data")
        last_date_in_datas = mongo_manager.get_last_data_date_from_one_collection(db_name=db_name, collection=collection)
        if last_date_in_datas != None:
            # Load the next day of the existing row
            start_date_to_load = pd.to_datetime(last_date_in_datas) + pd.Timedelta(days=1)
        else:
            print(f"[INFO] Not row in collection {collection}, so load all the data")
            if collection == "gas_stations_price_logs_eur" or collection == "denorm_station_prices":
                start_date_to_load = pd.to_datetime("2007-01-01")
            elif collection == "official_oils_prices" or collection == "denorm_station_vs_official_prices":
                start_date_to_load = pd.to_datetime("1985-01-01")
        end_date_to_load = pd.Timestamp.today().normalize()
        print(f"[INFO] start_date_to_load= {start_date_to_load}, end_date_to_load= {end_date_to_load}")
    return start_date_to_load, end_date_to_load


def drop_one_collection_to_mongo(db_name, collection_name):
    if db_name == None:
        print(f"[WARNING] db_name variable is empty")
        return "db_name variable is empty"
    db_name_exist = mongo_manager.does_database_exist(db_name)
    if not db_name_exist:
        print(f"[WARNING] db_name {db_name} does not exist on Mongo")
        return f"db_name {db_name} does not exist on Mongo"
    if collection_name == None:
        print(f"[WARNING] collection_name variable is empty")
        return "collection_name variable is empty"
    collection_name_exist = mongo_manager.does_collection_name_exist(db_name, collection_name)
    if not collection_name_exist:
        print(f"[WARNING] collection_name {collection_name} does not exist on {db_name} bdd Mongo")
        return f"collection_name {collection_name} does not exist on {db_name} bdd Mongo"
    mongo_manager.drop_mongo_collections(db_name, [collection_name])
    return "done"


def drop_one_bdd_to_mongo(db_name):
    if db_name == None:
        print(f"[WARNING] db_name variable is empty")
        return "db_name variable is empty"
    db_name_exist = mongo_manager.does_database_exist(db_name)
    if not db_name_exist:
        print(f"[WARNING] db_name {db_name} does not exist on Mongo")
        return f"db_name {db_name} does not exist on Mongo"
    mongo_manager.drop_mongo_bdd(db_name)
    return "done"


def save_mongo_dump_to_S3(db_name):
    if db_name == None:
        print(f"[WARNING] 'db_name' variable is empty — using default database names: 'datalake' and 'denormalization'.")
        db_names = ["datalake", "denormalization"]
    elif not isinstance(db_name, list):
        db_names = [db_name]
    else:
        db_names = db_name

    for db_name in db_names:
        db_name_exist = mongo_manager.does_database_exist(db_name)
        if not db_name_exist:
            print(f"[WARNING] db_name {db_name} does not exist on Mongo")
            return f"db_name {db_name} does not exist on Mongo"

        # clean working mongo_dump folder and recreate it
        if os.path.exists("outputs/mongo_dump"):
            shutil.rmtree("outputs/mongo_dump")
        os.makedirs("outputs/mongo_dump", exist_ok=True)

        str_current_date = datetime.now().strftime("%Y_%m_%d")
        folder_to_zip = "outputs/mongo_dump/"+ db_name +"_"+str_current_date
        # create mongo dump
        mongo_manager.mongodump(db_name, out_path=folder_to_zip)
        # zip the dump
        zip_path = compress_mongo_dump_to_zip(folder_to_zip, db_name)
        # load to S3
        S3_manager.upload_file_to_s3(file_path=zip_path)
    return "done"


def compress_mongo_dump_to_zip(folder_to_zip, db_name):
    if not os.path.exists(folder_to_zip):
        print(f"[ERROR]: Dump folder for '{db_name}' not found at {folder_to_zip}")

    shutil.make_archive(folder_to_zip, 'zip', folder_to_zip)
    print(f"[INFO] Success zip the dump: {folder_to_zip}.zip")
    # Remove the uncompressed dump folder
    shutil.rmtree(folder_to_zip)
    print(f"[INFO] Removed original uncompressed dump folder: {folder_to_zip}")
    return folder_to_zip + ".zip"


def restore_mongo_dump_from_S3(zip_name, new_bdd_name):
    # verify if mongo_dump name is existing in S3
    result, logs = S3_manager.check_existence_into_S3(zip_name)
    if result != True:
        return f'fail, {logs}'
    # download zip to S3
    os.makedirs("outputs/mongo_dump", exist_ok=True)
    S3_manager.download_file_from_s3_to_path(zip_name, out_path="outputs/mongo_dump")
    # dezip mongo dump
    dump_folder_path, old_db_name = decompress_zip_to_mongo_dump(zip_name)
    # restore bdd in mongo
    mongo_manager.mongorestore(dump_folder_path, old_db_name, new_bdd_name)
    return "done"


def decompress_zip_to_mongo_dump(zip_name, dump_folder="outputs/mongo_dump"):
    zip_path = os.path.join(dump_folder, zip_name)
    if not os.path.exists(zip_path):
        print(f"[ERROR]: Zip file for '{zip_name}' not found at {zip_path}")
    folder_path = os.path.join(dump_folder, zip_name.split(".zip")[0])
    shutil.unpack_archive(zip_path, folder_path)
    # Remove the zip dump file
    os.remove(zip_path)
    old_db_name = "_".join(zip_name.split("_")[:-3])
    return folder_path, old_db_name
