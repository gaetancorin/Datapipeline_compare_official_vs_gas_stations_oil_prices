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
    print("[INFO] Start launch_etl_gas_stations_oils_prices")
    if year_to_load != None and int(year_to_load) < 2007:
        print(f"[WARNING] {year_to_load} 'year_to_load' parameter is < 2007, so data is not available at this date for gas_stations_oils_prices")
        return "done"
    if drop_mongo_collections == "true":
        print("[INFO] Drop Mongo collections")
        mongo_manager.drop_mongo_collections(bdd = "datalake", collections= ["gas_stations_infos", "gas_stations_prices"])
    start_date_to_load, end_date_to_load = utils.determine_dates_to_load_from_mongo(year_to_load, db_name= "datalake", collection= "gas_stations_prices")
    extract_api_gas_stations_oils_prices(start_date_to_load, end_date_to_load)
    result = transform_gas_stations_oils_prices(start_date_to_load, end_date_to_load)
    if result == None:
        return "done"
    load_gas_stations_oils_prices_to_mongo()
    return "done"


def extract_api_gas_stations_oils_prices(start_date_to_load, end_date_to_load):
    print("[INFO] Start extract_api_gas_stations_oils_prices")
    start_year = start_date_to_load.year
    end_year = end_date_to_load.year
    years_to_load = list(range(start_year, end_year + 1))
    # years_to_load = ["2007", "2008"]

    # clean working xml/csv folders and recreate it
    if os.path.exists("outputs/stations_prices_source/xml"):
        shutil.rmtree("outputs/stations_prices_source/xml")
    os.makedirs("outputs/stations_prices_source/xml", exist_ok=True)
    if os.path.exists("outputs/stations_prices_source/csv"):
        shutil.rmtree("outputs/stations_prices_source/csv")
    os.makedirs("outputs/stations_prices_source/csv", exist_ok=True)

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
            zip_ref.extractall("outputs/stations_prices_source/xml")
        print(file_name, "extracted")

        # transform xml to df
        tree = ET.parse(f"outputs/stations_prices_source/xml/PrixCarburants_annuel_{year}.xml")
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

        # Standardize three different date formats into a single format ('%Y-%m-%d %H:%M:%S.%f')
        # (used between 2014 and now)
        df['maj_without_microsec_with_T'] = pd.to_datetime(df['maj'], format='%Y-%m-%dT%H:%M:%S',errors='coerce').dt.strftime('%Y_%m_%d_%H:%M')
        # (used between 2007 and 2013)
        df['maj_without_microsec'] = pd.to_datetime(df['maj'], format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.strftime('%Y_%m_%d_%H:%M')
        df['maj_with_microsec'] = pd.to_datetime(df['maj'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce').dt.strftime('%Y_%m_%d_%H:%M')
        # merge into same columns and clean
        df['maj'] = df['maj_with_microsec'].fillna(df['maj_without_microsec']).fillna(df['maj_without_microsec_with_T'])
        df = df.drop(columns=["maj_without_microsec", "maj_with_microsec", "maj_without_microsec_with_T"])

        # Standardize the "valeur" column:
        # - From 2022 to now, values are in the range 0.5 to 2.0
        # - From 2007 to 2021, values are in the range 500 to 2000 and must be scaled to match the new format
        df['valeur'] = df['valeur'] / 1000 if int(year) < 2022 else df['valeur']

        # Save df to csv
        df.to_csv(f"outputs/stations_prices_source/csv/stations_prices_source_{year}.csv", index=False)
        print(df.head(5))
        print("END LOAD", year)
    print("END LOAD", years_to_load)
    return "done"

def transform_gas_stations_oils_prices(start_date_to_load, end_date_to_load):
    print("[INFO] Start transform_gas_stations_oils_prices")

    # clean working folder and recreate it
    if os.path.exists("outputs/stations_prices_source/transformed"):
        shutil.rmtree("outputs/stations_prices_source/transformed")
    os.makedirs("outputs/stations_prices_source/transformed", exist_ok=True)

    files_names = os.listdir("outputs/stations_prices_source/csv")
    print(files_names)
    for file_name in files_names:
        print("Transform", file_name)
        file_path = f"outputs/stations_prices_source/csv/{file_name}"
        year = file_name.split("_")[-1].split(".")[0]
        if not os.path.exists(file_path):
            print(f"{file_path} file not exist.")
        else:
            # Get CSV gas_stations_oils_prices for the specific year, and transformed it
            df = pd.read_csv(file_path)
            print(df.head(5))

            # separate date to time
            df['date'] = pd.to_datetime(df['maj'], format='%Y_%m_%d_%H:%M', errors='coerce').dt.strftime('%Y_%m_%d')
            df["date"] = pd.to_datetime(df["date"], format="%Y_%m_%d")
            df['heuremin'] = pd.to_datetime(df['maj'],format='%Y_%m_%d_%H:%M',errors='coerce').dt.strftime('%H:%M')
            df = df.drop(columns=["maj"])

            # reduce df by specific dates we want only
            df = df[(df['date'] >= start_date_to_load) & (df['date'] <= end_date_to_load)]
            if df.empty:
                print(f"[INFO] No data in stations_prices_source between {start_date_to_load} and {end_date_to_load}")
                return None
            print(df.head(5))

            print("stations_prices_source oils availables:\n", df["nom"].dropna().unique())

            print('clean df by standard deviation and z-score')
            # for each day and type of oil, define standard deviation
            df["z_score"] = df.groupby(["date", "nom"])["valeur"].transform(lambda x: zscore(x, ddof=0))
            # delete all datas who have big standard deviation by z-score (abs change negatif value for positif)
            df_transformed = df[abs(df["z_score"]) < 1]
            df_transformed = df_transformed.drop(columns=["z_score"])

            # Keep only one data per minute
            df_transformed = df_transformed.sort_values(by=['id', 'date', 'nom', 'heuremin'])
            df_transformed = df_transformed.groupby(['id', 'date', 'nom', 'heuremin'], as_index=False).last()

            # change date and heuremin to str
            df_transformed["date"] = df_transformed["date"].dt.strftime('%Y_%m_%d')
            # df_transformed["heuremin"] = df_transformed["heuremin"].dt.strftime('%H:%M')

            # rename id to id_station_essence
            df_transformed["id_station_essence"] = df_transformed["id"]
            df_transformed = df_transformed.drop(columns=["id"])

            df_transformed.columns = [col.capitalize() for col in df_transformed.columns]

            # Save df to csv
            df_transformed.to_csv(f"outputs/stations_prices_source/transformed/stations_prices_transformed_{year}.csv", index=False)
            print(df_transformed.head(5))
            print("END LOAD", year)
    print("END LOAD", files_names)
    return "done"

def load_gas_stations_oils_prices_to_mongo():
    print("[INFO] Start load_gas_stations_oils_prices_to_mongo")

    # clean working csv folder and recreate it
    if os.path.exists("outputs/stations_infos"):
        shutil.rmtree("outputs/stations_infos")
    os.makedirs("outputs/stations_infos", exist_ok=True)
    if os.path.exists("outputs/stations_prices"):
        shutil.rmtree("outputs/stations_prices")
    os.makedirs("outputs/stations_prices", exist_ok=True)

    files_names = os.listdir("outputs/stations_prices_source/transformed")
    print(files_names)
    for file_name in files_names:
        print("Load", file_name)
        file_path = f"outputs/stations_prices_source/transformed/{file_name}"
        year = file_name.split("_")[-1].split(".")[0]
        if not os.path.exists(file_path):
            print(f"{file_path} file not exist.")
        else:
            df = pd.read_csv(file_path)
            print("rows =", len(df))
            df['Date'] = pd.to_datetime(df['Date'], format='%Y_%m_%d')

            # Split data to stations_infos and stations_prices
            # update stations_infos
            df_stations_infos = df[['Id_station_essence', 'Adresse', 'Ville', 'Cp', 'Latitude', 'Longitude', 'Date']]
            # Keep the latest Date(most recent) by Sort 'Id_station_essence' and 'Date' and keep the last
            df_stations_infos = df_stations_infos.sort_values(['Id_station_essence','Date'], ascending=[True, True])
            df_stations_infos = df_stations_infos.drop_duplicates(subset=['Id_station_essence'], keep='last').reset_index(drop=True)
            df_stations_infos = df_stations_infos.rename(columns={"Date": "Derniere_maj"})
            print('stations_infos \n', df_stations_infos.head(5))
            # Save df to csv
            df_stations_infos.to_csv(f"outputs/stations_infos/stations_infos_{year}.csv",index=False)
            # Save df to Mongo
            mongo_manager.update_gas_stations_infos(df_stations_infos, db_name= "datalake", collection= "gas_stations_infos")


            # add stations_prices
            df_stations_prices = df[['Date', 'Id_station_essence', 'Nom', 'Valeur', 'Heuremin']]
            print('stations_prices \n', df_stations_prices.head(5))
            # Save df to csv
            df_stations_prices.to_csv(f"outputs/stations_prices/stations_prices_{year}.csv", index=False)
            # Save df to Mongo
            result = mongo_manager.load_datas_to_mongo(df_stations_prices, bdd= "datalake", collection= "gas_stations_prices", index= ["Date", "Nom"])
            if result:
                print("correctly loaded", file_name, "on mongo collection 'stations_prices'")

        print("END LOAD", file_name)
    print("END LOAD", files_names)
    return "done"