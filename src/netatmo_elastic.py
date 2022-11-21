#!/usr/bin/env python3
# encoding=utf-8

from logging import exception
import signal
import argparse
import configparser
from time import sleep
from os import environ
from pathlib import Path
from datetime import datetime
from xmlrpc.client import Boolean
import pyatmo
import eland as ed
from elasticsearch import Elasticsearch
import pandas as pd

def parse_config(config_file=None):
    _config = configparser.ConfigParser(interpolation=None)

    if config_file is None:
        config_file = Path("config.ini")        

    if config_file.exists():
        _config.read(config_file)

    return _config

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", dest="config", type=str, nargs=1, required=False)
    return parser.parse_args()

def shutdown(_signal):
    global running
    running = False

def process_rain(es, index_name, station, station_name, backup_dir):
    # Convert Timestamps
    station['When'] =  pd.Timestamp.fromtimestamp(station['When'], 'UTC')

    station_data = pd.json_normalize(station)
    station_data = station_data.rename(columns={
                        'When' : '@timestamp',
                        'Rain' : 'rain'                         
                        })
    station_data["station_name"] = station_name

    # append data frame to CSV file
    station_data.to_csv(backup_dir+station_name+'.csv', mode='a', index=False, header=False)

    # Push Results to Elastic
    df = ed.pandas_to_eland(
        pd_df=station_data,
        es_client=es,
        es_dest_index=index_name,
        es_if_exists="append",
        es_type_overrides={
            '@timestamp' : 'date'
            },
        use_pandas_index_for_es_ids=False,
        es_refresh=True
    )

    return station_data

def process_station(es, index_name, station, station_name, backup_dir):
    # Convert from C to F
    station['Temperature'] = station['Temperature'] * 1.8 + 32
    if 'min_temp' in station:
        station['min_temp'] = station['min_temp'] * 1.8 + 32
    if 'max_temp' in station:
        station['max_temp'] = station['max_temp'] * 1.8 + 32

    station['Humidity'] = station['Humidity'] / 100

    if 'Pressure' in station:
        station['Pressure'] = float(station['Pressure'])
    if 'AbsolutePressure' in station:
        station['AbsolutePressure'] = float(station['AbsolutePressure'])
    if 'min_temp' in station:
        station['min_temp'] = float(station['min_temp'])
    if 'max_temp' in station:
        station['max_temp'] = float(station['max_temp'])

    # Convert Timestamps
    station['When'] = pd.Timestamp.utcfromtimestamp(station['When'])
    if 'date_min_temp' in station:
        station['date_min_temp'] = pd.Timestamp.utcfromtimestamp(station['date_min_temp'])
    if 'date_max_temp' in station:
        station['date_max_temp'] = pd.Timestamp.utcfromtimestamp(station['date_max_temp'])

    station_data = pd.json_normalize(station)
    station_data = station_data.rename(columns={
                        "Temperature": "temperature", 
                        'CO2' : 'co2',
                        'Humidity' : 'humidity',
                        'Noise' : 'noise',
                        'Pressure' : 'pressure',
                        'AbsolutePressure' : 'absolute_pressure',
                        'min_temp' : 'min_tempature',
                        'max_temp' : 'max_tempature',
                        'date_max_temp' : 'date_max_tempature',
                        'date_min_temp' : 'date_min_tempature',
                        'temp_trend' : 'tempature_trend',
                        'When' : '@timestamp'                        
                        })
    station_data["station_name"] = station_name
    
    # append data frame to CSV file
    station_data.to_csv(backup_dir+station_name+'.csv', mode='a', index=False, header=False)

    # Push Results to Elastic
    df = ed.pandas_to_eland(
        pd_df=station_data,
        es_client=es,
        es_dest_index=index_name,
        es_if_exists="append",
        es_type_overrides={
            '@timestamp' : 'date'
            },
        use_pandas_index_for_es_ids=False,
        es_refresh=True
    )

if __name__ == "__main__":
    running = True
    interval = None
    authorization = None
    client_id = None
    client_secret = None
    netatmo_username = None
    netatmo_password = None
    elastic_url = None
    elastic_username = None
    elastic_password = None
    elastic_verify_certs = True
    args = parse_args()
    config = parse_config(args.config)

    if environ.get("TERM"):
        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)

    if "global" in config:
        interval = int(config["global"]["interval"])

    if "netatmo" in config:
        client_id = config["netatmo"]["client_id"]
        client_secret = config["netatmo"]["client_secret"]
        netatmo_username = config["netatmo"]["netatmo_username"]
        netatmo_password = config["netatmo"]["netatmo_password"]
        netatmo_station_id = config["netatmo"]["netatmo_station_id"]
    
    if "elastic" in config:
        elastic_url = config["elastic"]["elastic_url"]
        elastic_username = config["elastic"]["elastic_username"]
        elastic_password = config["elastic"]["elastic_password"]
        ca_certs_dir = config["elastic"]["ca_certs_dir"]

    if environ.get("NETATMO_CLIENT_ID"):
        client_id = environ.get("NETATMO_CLIENT_ID")
    if environ.get("NETATMO_CLIENT_SECRET"):
        client_secret = environ.get("NETATMO_CLIENT_SECRET")
    if environ.get("NETATMO_USERNAME"):
        netatmo_username = environ.get("NETATMO_USERNAME")
    if environ.get("NETATMO_PASSWORD"):
        netatmo_password = environ.get("NETATMO_PASSWORD")
    if environ.get("NETATMO_STATION_ID"):
        netatmo_station_id = environ.get("NETATMO_STATION_ID")
        
    if environ.get("ELASTIC_URL"):
        elastic_url = environ.get("ELASTIC_URL")
    if environ.get("ELASTIC_USERNAME"):
        elastic_username = environ.get("ELASTIC_USERNAME")
    if environ.get("NETATMO_STATION_ID"):
        elastic_password = environ.get("ELASTIC_PASSWORD")
    if environ.get("CA_CERTS"):
        ca_certs_dir = environ.get("CA_CERTS")    

    if interval is None:
        interval = 300  # interval in seconds; default are 5 Minutes
    elif environ.get("INTERVAL"):
        interval = int(environ.get("INTERVAL"))

    if environ.get("BACKUP_DIR"):
        backup_dir = environ.get("BACKUP_DIR")
    else:
        backup_dir = config["global"]["backup_dir"]
    
    # ElasticSeach Connection
    es = Elasticsearch(
        elastic_url,
        basic_auth=(elastic_username, elastic_password),
        verify_certs=False, # currently using self signed certs
        ca_certs=ca_certs_dir
    )

    while running:
        authorization = pyatmo.ClientAuth(
            client_id=client_id,
            client_secret=client_secret,
            username=netatmo_username,
            password=netatmo_password,
            scope="read_station"
        )

        try:
            weather_data = pyatmo.WeatherStationData(authorization)
            weather_data.update()
            weather_current_data = weather_data.get_last_data(netatmo_station_id)

            # 0 - Primary Station
            # 1 - Outside Module
            # 2 - Rain Gauge
            # 3 - Main Floor
            # 4 - 2nd Floor
            stations = list(weather_current_data.keys())
            primary_station = weather_current_data[stations[0]]
            outside_station = weather_current_data[stations[1]]
            rain_gauge = weather_current_data[stations[2]]
            main_floor = weather_current_data[stations[3]]
            second_floor = weather_current_data[stations[4]]

            process_station(es,"netatmo_indoor", primary_station, 'Basement', backup_dir)
            process_station(es,"netatmo_outdoor", outside_station, 'Backyard', backup_dir)
            process_station(es, "netatmo_main_floor", main_floor, 'Main Floor', backup_dir)
            process_station(es, "netatmo_second_floor", second_floor, 'Second Floor', backup_dir)
            process_rain(es, "netatmo_rain_gauge", rain_gauge, 'Rain Gauge', backup_dir)
        except Exception as e:
            print("exception {}".format(e))
            # Print and wait the interval to try again

        sleep(interval)
