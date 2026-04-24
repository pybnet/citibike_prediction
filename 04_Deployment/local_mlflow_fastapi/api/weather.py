import requests
import json
from datetime import datetime
import pandas as pd
import http.client
from dotenv import load_dotenv
from pathlib import Path
import os

# ----- Charger le .env depuis un chemin précis -----
dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
if not RAPIDAPI_KEY:
    raise ValueError("RAPIDAPI_KEY not set in .env")

def station_weather_data() -> pd.DataFrame:
    conn = http.client.HTTPSConnection("meteostat.p.rapidapi.com")

    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': "meteostat.p.rapidapi.com"
    }

    NYC_station_id = "KNYC0"
    tz = "Europe/Berlin"
    url = f"/stations/hourly?station={NYC_station_id}&start={datetime.today().date()}&end={datetime.today().date()}&tz={tz}"

    conn.request("GET", url, headers=headers)
    res = conn.getresponse()

    weather_json = json.loads(res.read().decode("utf-8"))

    weather_data = weather_json["data"][-1]

    weather_df = pd.DataFrame(weather_data, index=[0])
    print('weather data loaded into dataframe')

    # Convert time
    weather_df["time"] = pd.to_datetime(weather_df["time"])

    # Rename columns
    weather_df = weather_df.rename(columns={
        "rhum": "relative_humidity",
        "prcp": "precipitation_total",
        "wspd": "average_wind_speed",
    })
    print('renaming columns done')
    
    weather_df['coco_group'] = weather_df['coco'].map({
        1: 'Pas de pluie', 2: 'Pas de pluie', 3: 'Pas de pluie',
        4: 'Pas de pluie', 5: 'Pas de pluie', 6: 'Pas de pluie',
        8: 'Pluie/Neige', 9: 'Pluie/Neige', 10: 'Pluie/Neige',
        11: 'Pluie/Neige', 12: 'Pluie/Neige', 13: 'Pluie/Neige',
        14: 'Pluie/Neige', 15: 'Pluie/Neige', 16: 'Pluie/Neige',
        18: 'Pluie/Neige', 19: 'Pluie/Neige', 20: 'Pluie/Neige',
        21: 'Pluie/Neige', 22: 'Pluie/Neige', 23: 'Pluie/Neige',
        24: 'Pluie/Neige', 25: 'Pluie/Neige', 26: 'Pluie/Neige',
        27: 'Pluie/Neige',
        17: 'Averse de pluie'
    })

    #remove useless columns
    keep_columns = ['temp', 'relative_humidity', 'precipitation_total', 'average_wind_speed', 'coco', 'coco_group'] 
    weather_df = weather_df[keep_columns]
    print('keeping only specified columns')
    
    return weather_df