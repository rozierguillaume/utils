
import pandas as pd
import json
import math
import requests
import numpy as np

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
    url = "https://public.opendatasoft.com/api/records/1.0/search/?dataset=arome-0025-enriched&q=&rows=-1&sort=forecast&facet=commune&facet=code_commune&refine.commune={}".format(ville)
    data = requests.get(url)
    with open('data/input/{}.json'.format(ville), 'wb') as f:
        f.write(data.content)

def prepare_data(df):
    df = df[['2_metre_temperature', 'relative_humidity', 'forecast']].groupby(['forecast']).mean()
    return df

def import_data(ville):
    with open('data/input/{}.json'.format(ville)) as f:
        data = json.load(f)
    return pd.DataFrame.from_dict([record['fields'] for record in data['records']])

def iterate_villes():
    dict_data = {}
    villes = ["Paris", "Grenoble"]
    for ville in villes:
        download_data(ville=ville)
        df = import_data(ville=ville)
        df = prepare_data(df).dropna()
        df["iptcc"] = calculer_iptcc(df)
        dict_data[ville] = {"forecast": df.index.tolist(),"temperature": df["2_metre_temperature"].round(1).tolist(), "humidite_relative": df.relative_humidity.round(1).tolist(), "iptcc": df.iptcc.round(1).tolist()}
    print(dict_data)

iterate_villes()