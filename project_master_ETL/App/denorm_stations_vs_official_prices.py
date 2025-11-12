import shutil
import os
import pandas as pd
import App.utils as utils
import App.mongo_manager as mongo_manager

def merge_denorm_stations_vs_official_prices(year_to_load = None, drop_mongo_collections = None):
    print("[INFO] Start merge_denorm_stations_vs_official_prices")
    if year_to_load != None and int(year_to_load) < 2007:
        print(f"[WARNING] {year_to_load} 'year_to_load' parameter is < 2007, so data is not available at this date for merge_denorm_stations_vs_official_prices")
        return "done"
    if drop_mongo_collections == "true":
        print("[INFO] Drop Mongo collections")
        mongo_manager.drop_mongo_collections(db_name = "denormalization", collections= ["denorm_stations_vs_official_prices"])
    start_date_to_load, end_date_to_load = utils.determine_dates_to_load_from_mongo(year_to_load, db_name= "denormalization", collection= "denorm_station_vs_official_prices")
    df_denorm_stations = extract_denorm_stations_prices_from_mongo(start_date_to_load, end_date_to_load)
    if df_denorm_stations.empty:
        return None
    df_denorm_official = extract_denorm_official_prices_from_mongo(start_date_to_load, end_date_to_load)
    if df_denorm_official.empty:
        return None
    denorm_stations_vs_official_prices = transform_merge_stations_vs_official_prices(df_denorm_stations, df_denorm_official)
    load_denorm_stations_vs_official_prices_to_mongo(denorm_stations_vs_official_prices)
    return "done"


def extract_denorm_stations_prices_from_mongo(start_date_to_load, end_date_to_load):
    print("[INFO] Start extract_denorm_stations_prices_from_mongo")

    # clean working csv folder and recreate it
    if os.path.exists("outputs/denorm_stations_vs_official_prices"):
        shutil.rmtree("outputs/denorm_stations_vs_official_prices")
    os.makedirs("outputs/denorm_stations_vs_official_prices", exist_ok=True)

    df_denorm_stations = mongo_manager.get_datas_by_date_from_one_collection(start_date_to_load, end_date_to_load, db_name="denormalization", collection="denorm_stations_prices")
    if df_denorm_stations.empty:
        print(f"[WARNING] Into merge, No existing datas on denorm_stations_prices for Date {start_date_to_load} to {end_date_to_load}")
        return df_denorm_stations
    df_denorm_stations = df_denorm_stations[[
        "Date", "station_ttc_GAZOLE_eur_liter",
        "station_ttc_SP95_eur_liter", "station_ttc_E10_eur_liter", "station_ttc_SP98_eur_liter", "station_ttc_E85_eur_liter", "station_ttc_GPLC_eur_liter"
    ]]
    df_denorm_stations['Date'] = pd.to_datetime(df_denorm_stations['Date'])

    print(f"[INFO] Found {len(df_denorm_stations)} rows into denorm_stations_prices between {start_date_to_load} and {end_date_to_load}")
    return df_denorm_stations


def extract_denorm_official_prices_from_mongo(start_date_to_load, end_date_to_load):
    print("[INFO] Start extract_denorm_official_prices_from_mongo")

    df_denorm_official = mongo_manager.get_datas_by_date_from_one_collection(start_date_to_load, end_date_to_load, db_name="denormalization", collection="denorm_official_prices")
    if df_denorm_official.empty:
        print(f"[WARNING] Into merge, No existing datas on denorm_official_prices for Date {start_date_to_load} to {end_date_to_load}")
        return df_denorm_official
    df_denorm_official = df_denorm_official[[
        "Date", "official_ttc_GAZOLE_eur_liter",
        "official_ttc_SP95_eur_liter", "official_ttc_E10_eur_liter", "official_ttc_SP98_eur_liter", "official_ttc_E85_eur_liter", "official_ttc_GPLC_eur_liter"
    ]]
    df_denorm_official['Date'] = pd.to_datetime(df_denorm_official['Date'])

    print(f"[INFO] Found {len(df_denorm_official)} rows into denorm_official_prices between {start_date_to_load} and {end_date_to_load}")
    return df_denorm_official


def transform_merge_stations_vs_official_prices(df_denorm_stations, df_denorm_official):
    print("[INFO] Start transform_merge_stations_vs_official_prices")
    df_denorm_stations['Date'] = pd.to_datetime(df_denorm_stations['Date'])
    df_denorm_official['Date'] = pd.to_datetime(df_denorm_official['Date'])

    # merge the 2 df together
    df_merge = pd.merge(df_denorm_official, df_denorm_stations, on='Date', how='outer')
    # df_merge = pd.merge(df_official_oils, df_denorm_stations, on='Date', how='outer', indicator=True)
    df_merge.sort_values(by='Date', inplace=True)

    # Need to create DIY filters because Metabase filters is not working fine (lot of bugs)
    df_merge['Day_of_week'] = df_merge['Date'].dt.day_name()
    df_merge['Month'] = df_merge['Date'].dt.month_name()
    df_merge['Year'] = df_merge['Date'].dt.year.astype(str)
    df_merge['DayMonth'] = df_merge['Date'].dt.day.astype(str) + df_merge['Date'].dt.month_name().str.lower()
    print("df_merge_stations_vs_official_prices\n", df_merge.head())
    return df_merge


def load_denorm_stations_vs_official_prices_to_mongo(denorm_stations_vs_official_prices):
    print("[INFO] Start load_denorm_stations_vs_official_prices_to_mongo")

    # Save df to csv
    start_year = denorm_stations_vs_official_prices['Date'].min().year
    end_year = denorm_stations_vs_official_prices['Date'].max().year
    denorm_stations_vs_official_prices.to_csv(f"outputs/denorm_stations_vs_official_prices/denorm_stations_vs_official_prices_{start_year}_{end_year}.csv", index=False)

    # Save df to Mongo
    result = mongo_manager.load_datas_to_mongo(denorm_stations_vs_official_prices, db_name="denormalization",collection="denorm_station_vs_official_prices", index=["Date"])
    if result:
        print(f"correctly loaded denorm_stations_vs_official_prices_{start_year}_{end_year} on mongo collection 'denorm_station_vs_official_prices'")

    print(f"END LOAD denorm_stations_vs_official_prices_{start_year}_{end_year}")
    return "done"