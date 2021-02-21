
import pandas as pd
import json
import math
import requests
import numpy as np

coord_villes = {
    "Paris": [48.8566, 2.35222],
    "Lyon": [45.764043, 4.835659],
    "Toulouse": [43.6043, 1.4437],
    "Bordeaux": [44.837789, -0.57918],
    "Strasbourg": [48.5734053, 7.7521113],
    "Lille": [50.633333, 3.066667],
    "Brest": [48.390394, -4.486076],
    "Marseille": [43.300000, 5.400000],
    "Rennes": [48.117266, -1.6777926],
    "Tours": [47.394144, 0.68484],
    "Le Mans": [48.00611, 0.199556],
    "Dijon": [47.322047, 5.04148],
    "Grenoble": [45.188529, 5.724524],
    "Ajaccio": [41.91, 8.73],
    "Limoges": [45.833619, 1.261105],
    "Rouen": [49.443232, 1.099971],
    "Clermont-Ferrand":[45.777222, 3.087025],
    "Metz": [49.1193089, 6.1757156],
    "Montpellier": [43.6, 3.8833],
    "Nice": [43.7101728, 7.2619532],
    "Reims":[49.258329, 4.031696],
    "Troyes": [48.2973451, 4.0744009],
    "Orleans": [47.902964, 1.909251],
    "Pau": [43.2951, -0.370797],
    "La Rochelle": [46.160329, -1.151139],
    "Perpignan": [42.6886591, 2.8948332],
    "Aurillac": [44.930953, 2.444997],
    "Nevers": [46.994, 3.156],
    "Nancy": [48.6833, 6.2],
    "Poitiers": [46.580224, 0.340375],
    "Nantes": [47.218371, -1.553621],
    "Vannes": [47.658236, -2.760847],
    "Caen": [49.183333, -0.350000],
    "Montélimar": [44.5667, 4.75],
    "Annecy": [45.899247, 6.129384],
    "Brive-la-Gaillarde": [45.159555, 1.533937],
    "Mulhouse": [47.750839, 7.335888]
}

def calculer_iptcc(df):
    T = df["2_metre_temperature"].values
    RH = df.relative_humidity.values

    AH_num = 6.112 * np.exp(17.67 * T / (T+243.5)) * RH * 2.1674 
    AH_den = 273.15 + T
    AH = AH_num / AH_den

    contenu_exp = (T-7.5)**2/196 + (RH-75)**2/625 + (AH-6)**2/2.89
    IPTCC = 100 * np.exp(-0.5 * contenu_exp)
    return IPTCC

def download_data(ville="Paris"):
    url ="https://public.opendatasoft.com/api/records/1.0/search/?dataset=arpege-05-sp1_sp2&q=&rows=-1&facet=forecast&geofilter.distance={}%2C{}%2C40000".format(coord_villes[ville][0], coord_villes[ville][1]) #"https://public.opendatasoft.com/api/records/1.0/search/?dataset=arome-0025-enriched&q=&rows=-1&sort=forecast&facet=commune&facet=code_commune&refine.commune={}".format(ville)
    data = requests.get(url)
    with open('data/input/{}.json'.format(ville), 'wb') as f:
        f.write(data.content)

def prepare_data(df):
    df = df[['2_metre_temperature', 'relative_humidity', 'forecast']].groupby(['forecast']).mean().reset_index()
    df["forecast"] = pd.to_datetime(df["forecast"])
    df = df[(df.forecast.dt.hour >= 6) & (df.forecast.dt.hour <= 18) ]
    df = df.resample('D', on='forecast').mean().reset_index()
    print(df)
    return df

def import_data(ville):
    with open('data/input/{}.json'.format(ville)) as f:
        data = json.load(f)
    return pd.DataFrame.from_dict([record['fields'] for record in data['records']])

def export_json(dict_data):
    with open("data/output/iptcc.json", "w") as outfile:
        outfile.write(json.dumps(dict_data))

def iterate_villes():
    dict_data = {"villes": list(coord_villes.keys())}
    villes = coord_villes
    for ville in villes:
        print(ville)
        download_data(ville=ville)
        df = import_data(ville=ville)
        df = prepare_data(df).dropna()
        df["iptcc"] = calculer_iptcc(df)
        dict_data[ville] = {"forecast": df["forecast"].astype(str).tolist(),"temperature": df["2_metre_temperature"].round(1).tolist(), 
                            "humidite_relative": df.relative_humidity.round(1).tolist(), 
                            "iptcc": df.iptcc.round(1).tolist(),
                            "coordonnees": coord_villes[ville]}
    print(dict_data)
    export_json(dict_data)

iterate_villes()