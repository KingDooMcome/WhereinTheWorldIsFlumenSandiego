#load GEOJSON into MONGODB
from pymongo import MongoClient
import numpy as np
import urllib
import datetime

import urllib2
import pandas as pd
def loadJSON():

    #Mongo setup 1982 week 2
    client = MongoClient('localhost', 27017)
    db = client.arch
    #query for data
    for year in range(1962,2016):
        week = 52
        print(week, year)
        query = ("https://data.cdc.gov/resource/vpby-h6ej.json?week="+str(week)+"&&year="+str(year)+"&&$$app_token=vClZmS1kdHvBqetW4NCbZqkmy")
        data = pd.read_json(query)
        #drop table
        data.drop('_1_24_years', axis=1, inplace=True)
        data.drop('_1_year', axis=1, inplace=True)
        data.drop('_25_44_years', axis=1, inplace=True)
        data.drop('_54_64_years', axis=1, inplace=True)
        data.drop('_65_years', axis=1, inplace=True)
        data.drop('week_ending_date', axis=1, inplace=True)

        ##mongo inserts
        for i in range(0, len(data)):
            fluRecord = {}
            fluRecord['all_deaths'] = data.iloc[i]['all_deaths']
            fluRecord['city'] = data.iloc[i]['city']
            fluRecord['region'] = data.iloc[i]['region']
            fluRecord['state'] = data.iloc[i]['state']
            fluRecord['week'] = data.iloc[i]['week']
            fluRecord['year'] = data.iloc[i]['year']
            fluRecord['pneumonia_and_influenza_deaths'] = data.iloc[i]['pneumonia_and_influenza_deaths']
            db.fluData.insert_one(fluRecord)

if __name__ == '__main__':
    loadJSON()

