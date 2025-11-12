import pandas as pd
import shutil
import os
import warnings
import requests
import App.utils as utils
import App.utils_bot as official_bot
import App.mongo_manager as mongo_manager

warnings.filterwarnings('ignore', category=RuntimeWarning)

def launch_etl_denormalize_official_oils_prices(year_to_load = None, drop_mongo_collections = None):
    print("[INFO] Start launch_etl_denormalize_official_oils_prices")
    if year_to_load != None and int(year_to_load) < 1985:
        print(f"[WARNING] {year_to_load} 'year_to_load' parameter is < 1985, so data is not available at this date for denorm_official_prices")
        return "done"
    if drop_mongo_collections == "true":
        print("[INFO] Drop Mongo collections")
        mongo_manager.drop_mongo_collections(db_name = "denormalization", collections= ["denorm_official_prices"])
    start_date_to_load, end_date_to_load = utils.determine_dates_to_load_from_mongo(year_to_load, db_name= "denormalization", collection= "denorm_official_prices")
    df_denorm_official_prices = extract_api_denorm_official_oils_prices()
    df_denorm_official_prices = transform_denorm_official_oils_prices(df_denorm_official_prices, start_date_to_load, end_date_to_load)
    if df_denorm_official_prices.empty:
        print(f"[INFO] No data in df_denorm_official_prices between {start_date_to_load} and {end_date_to_load}")
        return "done"
    load_denorm_official_oils_prices_to_mongo(df_denorm_official_prices)
    return "done"


def extract_api_denorm_official_oils_prices():
    print("[INFO] Start extract_api_denorm_official_oils_prices")

    # clean working csv folder and recreate it
    if os.path.exists("outputs/denorm_official_prices"):
        shutil.rmtree("outputs/denorm_official_prices")
    os.makedirs("outputs/denorm_official_prices", exist_ok=True)

    # Source = "https://www.ecologie.gouv.fr/politiques-publiques/prix-produits-petroliers" (gouvernemental opendata website)
    # example of url get by bot (always change because of UUID, so need to be scrapped)=
    # "https://www.ecologie.gouv.fr/simulator-energies/monitoring/export/59707a7b55c0012d0efade376d62a56d3c86129a"
    url = official_bot.get_url_for_download_denorm_official_oils_prices()

    # need to load file in local because of SSL certificate
    response = requests.get(url, verify=False)
    with open("outputs/denorm_official_prices/temp_file.xlsx", "wb") as f:
        f.write(response.content)
    df_denorm_official_prices = pd.read_excel("outputs/denorm_official_prices/temp_file.xlsx", sheet_name=1, skiprows=0)
    return df_denorm_official_prices


def transform_denorm_official_oils_prices(df_denorm_official_prices, start_date_to_load, end_date_to_load):
    print("[INFO] Start transform_denorm_official_oils_prices")

    # rename columns
    df_denorm_official_prices = df_denorm_official_prices.rename(columns={
        'Gazole €/l ttc': 'official_ttc_GAZOLE_eur_liter',
        'Super sans plomb 95 €/l ttc': 'official_ttc_SP95_eur_liter',
        'Super SP95 - E10 €/l ttc': 'official_ttc_E10_eur_liter',
        'Super sans plomb 98 €/l ttc': 'official_ttc_SP98_eur_liter',
        'Superéthanol E85 €/l ttc': 'official_ttc_E85_eur_liter',
        'GPL €/l ttc': 'official_ttc_GPLC_eur_liter',
    })

    df_denorm_official_prices['Date'] = pd.to_datetime(df_denorm_official_prices['Date'], dayfirst=True)

    # filter on target date
    start_date_to_load = pd.to_datetime(start_date_to_load)
    end_date_to_load = pd.to_datetime(end_date_to_load)
    df_denorm_official_prices = df_denorm_official_prices[
        (df_denorm_official_prices['Date'] >= start_date_to_load) &
        (df_denorm_official_prices['Date'] <= end_date_to_load)
        ]
    print('df_denorm_official_prices \n', df_denorm_official_prices.head(5))
    return df_denorm_official_prices


def load_denorm_official_oils_prices_to_mongo(df_denorm_official_prices):
    print("[INFO] Start load_denorm_official_oils_prices_to_mongo")

    # Save df to csv
    start_year = df_denorm_official_prices['Date'].min().year
    end_year = df_denorm_official_prices['Date'].max().year
    df_denorm_official_prices.to_csv(f"outputs/denorm_official_prices/denorm_official_prices_{start_year}_{end_year}.csv", index=False)

    # Save df to Mongo
    result = mongo_manager.load_datas_to_mongo(df_denorm_official_prices, db_name="denormalization",collection="denorm_official_prices", index=["Date"])
    if result:
        print(f"correctly loaded denorm_official_prices_{start_year}_{end_year} on mongo collection 'denorm_official_prices'")

    print(f"END LOAD denorm_official_prices_{start_year}_{end_year}")
    return "done"
