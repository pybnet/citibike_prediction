import pandas as pd
import requests
from pandas import json_normalize
from typing import List

STATION_INFORMATION_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
STATIONS_STATUS_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"

#Download station_information.json and return a normalized DataFrame
def download_station_information() -> pd.DataFrame:
    resp = requests.get(STATION_INFORMATION_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    all_stations_informations = pd.DataFrame(data["data"]["stations"])[
    ["short_name", "station_id", "name", "capacity", "lat", "lon"]
]
    all_stations_informations['short_name']= all_stations_informations['short_name'].apply(
    lambda s: f"{s.split('.')[0]}.{s.split('.')[1]}0" if '.' in s and len(s.split('.')[1]) == 1 else s)
        
    return all_stations_informations
    

#Look up station_id and short_name for a given station name
def get_station_id_and_short_name(df: pd.DataFrame, station_name: str):
    match = df.loc[df["short_name"] == station_name, ["station_id", "short_name"]]

    if match.empty:
        raise ValueError(f"Station name '{station_name}' not found")

    return match.iloc[0].to_dict()

#find station availability:
def load_station_status_df() -> pd.DataFrame:
    response = requests.get(STATIONS_STATUS_URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    stations = data["data"]["stations"]

    df = pd.json_normalize(stations)
    return df

#Return "num_bikes_available", "num_docks_available" for the selected station
def get_station_availability(df:pd.DataFrame, station_id: str) -> pd.Series:
    row = df.loc[df["station_id"] == station_id]
    if row.empty:
        raise ValueError(f"Station '{station_id}' not found")

    return row[["num_bikes_available", "num_docks_available"]].iloc[0]
