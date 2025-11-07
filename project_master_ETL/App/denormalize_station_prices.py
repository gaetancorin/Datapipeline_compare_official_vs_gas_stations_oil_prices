import shutil
import os
import pandas as pd
import App.utils as utils
import App.mongo_manager as mongo_manager

def denormalize_station_prices_for_dataviz(year_to_load = None, drop_mongo_collections = None):
    print("[INFO] Start denormalize_station_prices_for_dataviz")
    if year_to_load != None and int(year_to_load) < 2007:
        print(f"[WARNING] {year_to_load} 'year_to_load' parameter is < 2007, so data is not available at this date for denormalize_station_prices_for_dataviz")
        return "done"
    if drop_mongo_collections == "true":
        print("[INFO] Drop Mongo collections")
        mongo_manager.drop_mongo_collections(bdd = "denormalization", collections= ["denorm_station_prices"])

    # clean working csv folder and recreate it
    if os.path.exists("outputs/denorm_station_prices"):
        shutil.rmtree("outputs/denorm_station_prices")
    os.makedirs("outputs/denorm_station_prices", exist_ok=True)

    start_date_to_load, end_date_to_load = utils.determine_dates_to_load_from_mongo(year_to_load, db_name= "denormalization", collection= "denorm_station_prices")

    current_year_to_load = start_date_to_load
    while current_year_to_load < end_date_to_load:
        start_date = current_year_to_load
        end_date = current_year_to_load + pd.DateOffset(years=1)
        if end_date >= end_date_to_load:
            end_date = end_date_to_load
        print(f"[INFO] Start denormalize station prices for year {start_date.year}")
        df_stations_price_logs = extract_new_station_prices_from_mongo(start_date, end_date)
        if df_stations_price_logs.empty:
            print(f"[WARNING] No found data in 'gas_stations_price_logs_eur' collection  between {start_date} et {end_date}")
        else:
            df_denorm_station_prices = transform_and_denormalize_station_prices(df_stations_price_logs)
            load_denormalized_station_prices(df_denorm_station_prices)
        current_year_to_load = current_year_to_load + pd.DateOffset(years=1)
    return "done"


def extract_new_station_prices_from_mongo(start_date_to_load, end_date_to_load):
    print("[INFO] Start extract_new_station_prices_from_mongo")
    df_stations_price_logs = mongo_manager.get_filtered_datas_from_one_collection(start_date_to_load, end_date_to_load, db_name="datalake", collection="gas_stations_price_logs_eur")
    if df_stations_price_logs.empty:
        print(f"[WARNING] No existing datas on gas_stations_price_logs_eur for Date {start_date_to_load} to {end_date_to_load}")
        return df_stations_price_logs
    df_stations_price_logs = df_stations_price_logs[["Id_station_essence", "Date", "Nom", "Valeur"]]

    print(f"[INFO] Found {len(df_stations_price_logs)} rows into gas_stations_price_logs_eur between {start_date_to_load} and {end_date_to_load}")
    return df_stations_price_logs


def transform_and_denormalize_station_prices(df_stations_price_logs):
    print("[INFO] Start transform_and_denormalize_station_prices")
    df_denorm_station_prices = df_stations_price_logs.rename(columns={
        "Id_station_essence": "Gas_station_id", "Nom": "Gas_name", "Valeur": "Gas_eur_liter"
    })
    df_denorm_station_prices['Date'] = pd.to_datetime(df_denorm_station_prices['Date'])

    # Combine the gas station prices when same date and same gas name (by mean)
    df_denorm_station_prices = df_denorm_station_prices.groupby(['Date', 'Gas_name'], as_index=False)['Gas_eur_liter'].mean().round(5)

    # Create col for each gas_name (pivot rotate df)
    df_denorm_station_prices = df_denorm_station_prices.pivot(index='Date', columns='Gas_name', values='Gas_eur_liter')
    df_denorm_station_prices = df_denorm_station_prices.rename(
        columns=lambda x: f"station_ttc_{x.upper()}_eur_liter").reset_index()
    # print("df_denorm_station_prices\n", df_denorm_station_prices)

    # need to always have all existing columns
    all_fuel_name_existing = [
        "station_ttc_E10_eur_liter",
        "station_ttc_E85_eur_liter",
        "station_ttc_GPLC_eur_liter",
        "station_ttc_GAZOLE_eur_liter",
        "station_ttc_SP95_eur_liter",
        "station_ttc_SP98_eur_liter"
    ]
    for col in all_fuel_name_existing:
        if col not in df_denorm_station_prices.columns:
            df_denorm_station_prices[col] = float('nan')

    print("df_denorm_station_prices\n", df_denorm_station_prices.head())

    return df_denorm_station_prices


def load_denormalized_station_prices(df_denorm_station_prices):
    print("[INFO] Start load_denorm_station_prices")

    # Save df to csv
    start_year = df_denorm_station_prices['Date'].min().year
    end_year = df_denorm_station_prices['Date'].max().year
    df_denorm_station_prices.to_csv(f"outputs/denorm_station_prices/denorm_station_prices_{start_year}_{end_year}.csv", index=False)

    # Save df to Mongo
    result = mongo_manager.load_datas_to_mongo(df_denorm_station_prices, bdd="denormalization",collection="denorm_station_prices", index=["Date"])
    if result:
        print(f"correctly loaded denorm_station_prices_{start_year}_{end_year} on mongo collection 'denormalization'")

    print(f"END LOAD denorm_station_prices_{start_year}_{end_year}")
    return "done"
