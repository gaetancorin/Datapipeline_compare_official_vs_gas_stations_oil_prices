import pandas as pd
import numpy as np
from scipy.stats import zscore
import requests
import shutil
import zipfile
import io
import os
import warnings
import time
import xml.etree.ElementTree as ET
import App.utils as utils
import App.mongo_manager as mongo_manager

warnings.filterwarnings('ignore', category=RuntimeWarning)

def launch_etl_gas_stations_oils_prices(year_to_load = None, drop_mongo_collections = None):
    print("[INFO] Start launch_etl_gas_stations_oil_prices")
    if year_to_load != None and int(year_to_load) < 2007:
        print(f"[WARNING] {year_to_load} 'year_to_load' parameter is < 2007, so data is not available at this date for gas_stations_oils_prices")
        return "done"
    if drop_mongo_collections == "true":
        print("[INFO] Drop Mongo collections")
        mongo_manager.drop_mongo_collections(bdd = "datalake", collections= ["gas_stations_infos", "gas_stations_price_logs_eur"])
    start_date_to_load, end_date_to_load = utils.determine_dates_to_load_from_mongo(year_to_load, db_name= "datalake", collection= "gas_stations_price_logs_eur")
    extract_new_gas_stations_oils_prices(start_date_to_load, end_date_to_load)
    result = transform_gas_stations_oils_prices(start_date_to_load, end_date_to_load)
    if result == None:
        return "done"
    load_gas_stations_oils_prices_to_mongo()
    return "done"


def extract_new_gas_stations_oils_prices(start_date_to_load, end_date_to_load):
    print("[INFO] Start extract_new_gas_stations_oil_prices")
    start_year = start_date_to_load.year
    end_year = end_date_to_load.year
    years_to_load = list(range(start_year, end_year + 1))
    # years_to_load = ["2007", "2008"]

    # clean working xml/csv folders and recreate it
    if os.path.exists("outputs/xml_gas_stations"):
        shutil.rmtree("outputs/xml_gas_stations")
    os.makedirs("outputs/xml_gas_stations", exist_ok=True)
    if os.path.exists("outputs/original_gas_stations"):
        shutil.rmtree("outputs/original_gas_stations")
    os.makedirs("outputs/original_gas_stations", exist_ok=True)

    # Loading targeting datas
    for year in years_to_load:
        print("LOAD", year)
        # Source = "https://www.prix-carburants.gouv.fr/rubrique/opendata/" (gouvernemental opendata website)
        url = f"https://donnees.roulez-eco.fr/opendata/annee/{year}"
        for count in range(3):
            try:
                response = requests.get(url)
                print(response)
                break
            except requests.exceptions.RequestException as e:
                print(f"{count} Retry fail due to:", e)
                time.sleep(5)

        # get filename
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        file_name = zip_file.namelist()[0]
        print(file_name)

        # save original xml file in local
        with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
            zip_ref.extractall("outputs/xml_gas_stations")
        print(file_name, "extracted")

        # transform xml to df
        tree = ET.parse(f"outputs/xml_gas_stations/PrixCarburants_annuel_{year}.xml")
        root = tree.getroot()
        data = []
        for pdv in root.findall("pdv"):
            for p in pdv.findall("prix"):
                row = {
                    "id": pdv.get("id"),
                    "latitude": pdv.get("latitude"),
                    "longitude": pdv.get("longitude"),
                    "cp": pdv.get("cp"),
                    "ville": pdv.find("ville").text if pdv.find("ville") is not None and pdv.find(
                        "ville").text else None,
                    "adresse": pdv.find("adresse").text.replace(",", " ").replace(";", " ") if pdv.find(
                        "adresse") is not None and pdv.find("adresse").text else None,
                    "nom": p.get("nom"),
                    "maj": p.get("maj"),
                    "valeur": p.get("valeur")
                }
                data.append(row)
        df = pd.DataFrame(data)

        ###### CLEAN DF AND ADD TYPES ##############
        df["id"] = df["id"].astype(int)
        df["latitude"] = df["latitude"].replace(["", "0"], np.nan).astype(float)
        df["longitude"] = df["longitude"].replace(["", "0"], np.nan).astype(float)
        df['adresse'] = df['adresse'].str.replace("\n", " ", regex=True)
        df["valeur"] = df["valeur"].astype(float)

        # (The CP is '35***' for one of the entries from 2008, and its ID "35200004" doesn't match any station in other years, so we remove it.)
        df = df[df["cp"] != "35***"]
        df["cp"] = df["cp"].replace("", np.nan).astype(int)

        # Standardize three different date formats into a single column
        # (used between 2014 and now)
        df['maj_without_microsec_with_T'] = pd.to_datetime(df['maj'], format='%Y-%m-%dT%H:%M:%S',errors='coerce').dt.strftime('%Y_%m_%d_%H:%M')
        # (used between 2007 and 2013)
        df['maj_without_microsec'] = pd.to_datetime(df['maj'], format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.strftime('%Y_%m_%d_%H:%M')
        df['maj_with_microsec'] = pd.to_datetime(df['maj'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce').dt.strftime('%Y_%m_%d_%H:%M')
        # merge into same columns and clean
        df['maj'] = df['maj_with_microsec'].fillna(df['maj_without_microsec']).fillna(df['maj_without_microsec_with_T'])
        df = df.drop(columns=["maj_without_microsec", "maj_with_microsec", "maj_without_microsec_with_T"])

        # Standardize the "valeur" column:
        # - From 2007 to 2021, values are in the range 500 to 2000
        # - From 2022 to now, values are in the range 0.5 to 2.0 and must be scaled to match the previous format
        df['valeur'] = df['valeur'] * 1000 if int(year) > 2021 else df['valeur']

        # Save df to csv
        df.to_csv(f"outputs/original_gas_stations/PrixCarburants_annuel_{year}.csv", index=False)
        print(df.head(5))
        print("END LOAD", year)
    print("END LOAD", years_to_load)
    return "done"

def transform_gas_stations_oils_prices(start_date_to_load, end_date_to_load):
    print("[INFO] Start transform_gas_stations_oils_prices")

    # clean working csv folder and recreate it
    if os.path.exists("outputs/filtered_gas_stations"):
        shutil.rmtree("outputs/filtered_gas_stations")
    os.makedirs("outputs/filtered_gas_stations", exist_ok=True)

    files_names = os.listdir("outputs/original_gas_stations")
    print(files_names)
    for file_name in files_names:
        print("Transform", file_name)
        file_path = f"outputs/original_gas_stations/{file_name}"
        year = file_name.split("_")[-1].split(".")[0]
        if not os.path.exists(file_path):
            print(f"{file_path} file not exist.")
        else:
            df_prices = pd.read_csv(file_path)
            print(df_prices.head(5))

            df_prices['date'] = pd.to_datetime(df_prices['maj'], format='%Y_%m_%d_%H:%M', errors='coerce').dt.strftime('%Y_%m_%d')
            df_prices["date"] = pd.to_datetime(df_prices["date"], format="%Y_%m_%d")
            df_prices['heuremin'] = pd.to_datetime(df_prices['maj'],format='%Y_%m_%d_%H:%M',errors='coerce').dt.strftime('%H:%M')
            df_prices = df_prices.drop(columns=["maj"])

            # reduce df by specific dates we want only
            df_prices = df_prices[
                (df_prices['date'] >= start_date_to_load) &
                (df_prices['date'] <= end_date_to_load)
                ]
            if df_prices.empty:
                print(f"[INFO] No data in gas_stations_oils_prices between {start_date_to_load} and {end_date_to_load}")
                return None
            print(df_prices.head(5))

            print("gas-station oils availables:\n", df_prices["nom"].dropna().unique())

            print('clean df by standard deviation and z-score')
            # for each day and type of oil, define standard deviation
            df_prices["z_score"] = df_prices.groupby(["date", "nom"])["valeur"].transform(lambda x: zscore(x, ddof=0))
            # delete all datas who have big standard deviation by z-score
            df_prices_filtered = df_prices[abs(df_prices["z_score"]) < 1]
            df_prices_filtered = df_prices_filtered.drop(columns=["z_score"])

            print('reduce df by keeping only last datas by days, oils and stations')
            # keep only last value of each day and each oil for each station to reduce datas (after z-score cleaning)
            df_prices_filtered['heuremin'] = pd.to_datetime(df_prices_filtered['heuremin'], format='%H:%M',errors='coerce')
            df_prices_filtered = df_prices_filtered.sort_values(by=['id', 'date', 'nom', 'heuremin'])
            df_prices_filtered = df_prices_filtered.groupby(['id', 'date', 'nom'], as_index=False).last()

            # change date and heuremin to str
            df_prices_filtered["date"] = df_prices_filtered["date"].dt.strftime('%Y_%m_%d')
            df_prices_filtered["heuremin"] = df_prices_filtered["heuremin"].dt.strftime('%H:%M')

            # rename id to id_station_essence
            df_prices_filtered["id_station_essence"] = df_prices_filtered["id"]
            df_prices_filtered = df_prices_filtered.drop(columns=["id"])

            # transform oil prices 500-2000 to 0.5-2.0
            df_prices_filtered['valeur'] = (df_prices_filtered['valeur'] * 0.001).round(5)

            df_prices_filtered.columns = [col.capitalize() for col in df_prices_filtered.columns]

            # Save df to csv
            df_prices_filtered.to_csv(f"outputs/filtered_gas_stations/PrixCarburants_annuel_filtered_{year}.csv", index=False)
            print(df_prices_filtered.head(5))
            print("END LOAD", year)
    print("END LOAD", files_names)
    return "done"

def load_gas_stations_oils_prices_to_mongo():
    print("[INFO] Start load_gas_stations_oil_prices_to_mongo")

    # clean working csv folder and recreate it
    if os.path.exists("outputs/gas_stations_infos"):
        shutil.rmtree("outputs/gas_stations_infos")
    os.makedirs("outputs/gas_stations_infos", exist_ok=True)
    if os.path.exists("outputs/gas_stations_price_logs_eur"):
        shutil.rmtree("outputs/gas_stations_price_logs_eur")
    os.makedirs("outputs/gas_stations_price_logs_eur", exist_ok=True)

    files_names = os.listdir("outputs/filtered_gas_stations")
    print(files_names)
    for file_name in files_names:
        print("Load", file_name)
        file_path = f"outputs/filtered_gas_stations/{file_name}"
        year = file_name.split("_")[-1].split(".")[0]
        if not os.path.exists(file_path):
            print(f"{file_path} file not exist.")
        else:
            df = pd.read_csv(file_path)
            print("rows =", len(df))
            df['Date'] = pd.to_datetime(df['Date'], format='%Y_%m_%d')

            # Split data to gas_stations_infos and gas_stations_price_logs
            # update gas_stations_infos
            df_gas_stations = df[['Id_station_essence', 'Adresse', 'Ville', 'Cp', 'Latitude', 'Longitude', 'Date']]
            # Keep the latest Date(most recent) by Sort 'Id_station_essence' and 'Date' and keep the last
            df_gas_stations = df_gas_stations.sort_values(['Id_station_essence','Date'], ascending=[True, True])
            df_gas_stations = df_gas_stations.drop_duplicates(subset=['Id_station_essence'], keep='last').reset_index(drop=True)
            df_gas_stations = df_gas_stations.rename(columns={"Date": "Derniere_maj"})
            print('gas_stations_infos \n', df_gas_stations.head(5))
            # Save df to csv
            df_gas_stations.to_csv(f"outputs/gas_stations_infos/gas_stations_infos_{year}.csv",index=False)
            # Save df to Mongo
            mongo_manager.update_gas_stations_infos(df_gas_stations, db_name= "datalake", collection= "gas_stations_infos")


            # add gas_stations_price_logs
            df_gas_stations_price_logs = df[['Date', 'Id_station_essence', 'Nom', 'Valeur', 'Heuremin']]
            print('gas_station_prices_logs \n', df_gas_stations_price_logs.head(5))
            # Save df to csv
            df_gas_stations_price_logs.to_csv(f"outputs/gas_stations_price_logs_eur/gas_station_prices_logs_eur_{year}.csv", index=False)
            # Save df to Mongo
            result = mongo_manager.load_datas_to_mongo(df_gas_stations_price_logs, bdd= "datalake", collection= "gas_stations_price_logs_eur", index= ["Date", "Nom"])
            if result:
                print("correctly loaded", file_name, "on mongo collection 'gas_stations_price_logs_eur'")

        print("END LOAD", file_name)
    print("END LOAD", files_names)
    return "done"