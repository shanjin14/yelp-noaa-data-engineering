import requests as re
import pandas as pd
import numpy as np
import json
from datetime import datetime
import time
from airflow.macros import ds_add
import os
import csv


def GetAllCityCode(Token):
    FullResult=[]
    for i in range(100):
        offset=i*1000
        req_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/locations?locationcategoryid=CITY&limit=1000&offset={}'.format(offset)
        locations = re.get(req_url, headers={'token':Token})
        d = json.loads(locations.text)
        if len(d)==0:
            print("empty string exit")
            break
        FullResult.extend(d['results'])
    #print(FullResult)
    return pd.DataFrame(FullResult).drop_duplicates()

def GetLongLat(inToken,station_id):
    """
    arg: inToken,stationid
    return long lat 
    """
    #print(station_id)
    req_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations/'+station_id
    longlat = re.get(req_url, headers={'token':inToken})
    json_longlat=json.loads(longlat.text)
    time.sleep(0.1)
    long =99.0
    lat = 99.0
    try:
        long = json_longlat['longitude']
        lat = json_longlat['latitude']
    except:
        print("no long lat info ",json_longlat)
    return long,lat

def GetPrecipitation(inToken,station_id,start_date,end_date):
    """
    arg: inToken,stationid
    return df with the data 
    """
    req_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=PRCP&limit=1000' \
        '&startdate='+start_date+'&enddate='+end_date+'&stationid='+station_id
    #print(req_url)
    prcp = re.get(req_url, headers={'token':inToken})
    if len(prcp.text)==2:  #2 character {}
        return pd.DataFrame(columns = ['attributes','datatype','date','station','prcp'])
    print(station_id,prcp.text)
    json_prcp = json.loads(prcp.text)
    df_prcp = pd.DataFrame(json_prcp['results'])
    df_prcp=df_prcp.rename(columns={"value":"prcp"})
    return df_prcp

def GetTemperature(inToken,location_id,city_name,start_date,end_date):
    req_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=TAVG&limit=1000&locationid='+location_id+ \
            '&startdate='+start_date+'&enddate='+end_date
    weather = re.get(req_url, headers={'token':inToken})
    time.sleep(0.1) #avoid next API request got cancelled due to hit the 5 req per sec quota
    #print(len(weather.text))
    if len(weather.text)==2:  #2 character {}, no data move on
        print(len(weather.text))
        return None
    json_weather = json.loads(weather.text)
    df_weather = pd.DataFrame(json_weather['results'])
    df_weather['location_id']=location_id
    df_weather['city_name']=city_name
    df_weather['average_temp']=df_weather['value'] / 10 #get degree celsius in correct decimal. Data returned is in *10 format
    station_unique = df_weather['station'].unique() #apply to each row is better as big city has multiple read
    print(location_id,city_name,station_unique)
    #get long lat
    long_lats = [GetLongLat(inToken,i) for i in station_unique]
    df_stn_long_lat=pd.DataFrame({'station_id':station_unique,'long_lat':long_lats})
    df_stn_long_lat[['long', 'lat']] = pd.DataFrame(df_stn_long_lat['long_lat'].tolist(), index=df_stn_long_lat.index)
    df_stn_long_lat=df_stn_long_lat.drop(['long_lat'],axis=1)
    print('remove empty long lat',len(df_stn_long_lat[(df_stn_long_lat['long'] ==99) & (df_stn_long_lat['lat'] ==99)]))
    df_stn_long_lat = df_stn_long_lat[(df_stn_long_lat['long'] !=99) & (df_stn_long_lat['lat'] !=99)]
    df_weather = df_weather.merge(df_stn_long_lat,  how='left', left_on='station', right_on='station_id')
    #precipitation
    list_prcp = [GetPrecipitation(inToken,i,start_date,end_date) for i in station_unique]
    #print(list_prcp)
    df_prcp = pd.concat(list_prcp)
    df_weather = df_weather.merge(df_prcp,  how='left', left_on=['station','date'], right_on = ['station','date'])
    #print(df_prcp.head())
    #subset the data
    df_weather=df_weather[['date','location_id','city_name','average_temp','station','long','lat','prcp']]
    df_weather['prcp'] = df_weather['prcp'].fillna(0)
    df_weather['city_name'] = df_weather['city_name'].str.replace(',', '')
    return df_weather

def acquire_temperature_data(inToken,*args, **kwargs):
    #Token = 'pMnUqnCVOhzBBBgneXwexkNCSaxdafuL'
    Token =inToken
    cities= GetAllCityCode(Token)
    cities=cities[cities['name'].str.contains("US")] #do only US cities [0:5]
    #print(cities.head())
    start_time=time.time()
    execution_date = kwargs["ds"]
    start_date=ds_add(execution_date, -4) #do 4 days ago until today
    end_date=ds_add(execution_date, -2)
    print(' execution date ',end_date)
    full_data = pd.concat([GetTemperature(Token,x, y,start_date,end_date) for x, y in zip(cities['id'], cities['name'])])
    print(time.time()-start_time)
    ts = pd.to_datetime(full_data['date'], format='%Y-%m-%dT%H:%M:%S')#.dt.strftime('%Y-%m-%d')
    for i, x in full_data.groupby(ts):
        outdir = '{}/{:02}/{:02}'.format(i.year,i.month,i.day)
        if not os.path.exists(outdir):
            os.makedirs(outdir, exist_ok=True)
        print('file path',outdir+'/temperature_data.csv')
        x.to_csv(outdir+'/temperature_data.csv', index=False, quotechar='"',quoting=csv.QUOTE_NONNUMERIC)