import shutil
import os
import pandas as pd
import App.utils as utils
import App.mongo_manager as mongo_manager

def merge_denorm_station_vs_official_prices(year_to_load = None, drop_mongo_collections = None):
    print("[INFO] Start merge_denorm_station_vs_official_prices")
    if year_to_load != None and int(year_to_load) < 2007:
        print(f"[WARNING] {year_to_load} 'year_to_load' parameter is < 2007, so data is not available at this date for merge_denorm_station_vs_official_prices")
        return "done"
    if drop_mongo_collections == "true":
        print("[INFO] Drop Mongo collections")
        mongo_manager.drop_mongo_collections(bdd = "denormalization", collections= ["denorm_station_vs_official_prices"])
    start_date_to_load, end_date_to_load = utils.determine_dates_to_load_from_mongo(year_to_load, db_name= "denormalization", collection= "denorm_station_vs_official_prices")
    df_denorm_station = extract_new_denorm_station_prices_from_mongo(start_date_to_load, end_date_to_load)
    if df_denorm_station.empty:
        return None
    df_official_oils = extract_new_official_oils_prices_from_mongo(start_date_to_load, end_date_to_load)
    if df_official_oils.empty:
        return None
    denorm_station_vs_official_prices = transform_merge_station_vs_official_prices(df_denorm_station, df_official_oils)
    load_denorm_station_vs_official_prices(denorm_station_vs_official_prices)
    return "done"


def extract_new_denorm_station_prices_from_mongo(start_date_to_load, end_date_to_load):
    print("[INFO] Start extract_new_denorm_station_prices_from_mongo")

    # clean working csv folder and recreate it
    if os.path.exists("outputs/denorm_station_vs_official_prices"):
        shutil.rmtree("outputs/denorm_station_vs_official_prices")
    os.makedirs("outputs/denorm_station_vs_official_prices", exist_ok=True)

    df_denorm_station = mongo_manager.get_filtered_datas_from_one_collection(start_date_to_load, end_date_to_load, db_name="denormalization", collection="denorm_station_prices")
    if df_denorm_station.empty:
        print(f"[WARNING] Into merge, No existing datas on denorm_station_prices for Date {start_date_to_load} to {end_date_to_load}")
        return df_denorm_station
    df_denorm_station = df_denorm_station[[
        "Date", "station_ttc_GAZOLE_eur_liter",
        "station_ttc_SP95_eur_liter", "station_ttc_E10_eur_liter", "station_ttc_SP98_eur_liter", "station_ttc_E85_eur_liter", "station_ttc_GPLC_eur_liter"
    ]]
    df_denorm_station['Date'] = pd.to_datetime(df_denorm_station['Date'])

    print(f"[INFO] Found {len(df_denorm_station)} rows into denorm_station_prices between {start_date_to_load} and {end_date_to_load}")
    return df_denorm_station


def extract_new_official_oils_prices_from_mongo(start_date_to_load, end_date_to_load):
    print("[INFO] Start extract_new_official_oils_prices_from_mongo")

    df_official_oils = mongo_manager.get_filtered_datas_from_one_collection(start_date_to_load, end_date_to_load, db_name="datalake", collection="official_oils_prices")
    if df_official_oils.empty:
        print(f"[WARNING] Into merge, No existing datas on official_oils_prices for Date {start_date_to_load} to {end_date_to_load}")
        return df_official_oils
    df_official_oils = df_official_oils[[
        "Date", "official_ttc_GAZOLE_eur_liter",
        "official_ttc_SP95_eur_liter", "official_ttc_E10_eur_liter", "official_ttc_SP98_eur_liter", "official_ttc_E85_eur_liter", "official_ttc_GPLC_eur_liter"
    ]]
    df_official_oils['Date'] = pd.to_datetime(df_official_oils['Date'])

    print(f"[INFO] Found {len(df_official_oils)} rows into official_oils_prices between {start_date_to_load} and {end_date_to_load}")
    return df_official_oils


def transform_merge_station_vs_official_prices(df_denorm_station, df_official_oils):
    print("[INFO] Start transform_merge_station_vs_official_prices")
    df_denorm_station['Date'] = pd.to_datetime(df_denorm_station['Date'])
    df_official_oils['Date'] = pd.to_datetime(df_official_oils['Date'])

    # merge the 2 df together
    df_merge = pd.merge(df_official_oils, df_denorm_station, on='Date', how='outer')
    # df_merge = pd.merge(df_official_oils, df_denorm_station, on='Date', how='outer', indicator=True)
    df_merge.sort_values(by='Date', inplace=True)
    df_merge['Day_of_week'] = df_merge['Date'].dt.day_name()
    df_merge['Month'] = df_merge['Date'].dt.month_name()
    df_merge['Year'] = df_merge['Date'].dt.year.astype(str)
    df_merge['DayMonth'] = df_merge['Date'].dt.day.astype(str) + df_merge['Date'].dt.month_name().str.lower()
    print("df_merge_station_vs_official_prices\n", df_merge.head())
    return df_merge


def load_denorm_station_vs_official_prices(denorm_station_vs_official_prices):
    print("[INFO] Start load_denorm_station_vs_official_prices")

    # Save df to csv
    start_year = denorm_station_vs_official_prices['Date'].min().year
    end_year = denorm_station_vs_official_prices['Date'].max().year
    denorm_station_vs_official_prices.to_csv(f"outputs/denorm_station_vs_official_prices/denorm_station_vs_official_prices_{start_year}_{end_year}.csv", index=False)

    # Save df to Mongo
    result = mongo_manager.load_datas_to_mongo(denorm_station_vs_official_prices, bdd="denormalization",collection="denorm_station_vs_official_prices", index=["Date"])
    if result:
        print(f"correctly loaded denorm_station_vs_official_prices_{start_year}_{end_year} on mongo collection 'denormalization'")

    print(f"END LOAD denorm_station_vs_official_prices_{start_year}_{end_year}")
    return "done"