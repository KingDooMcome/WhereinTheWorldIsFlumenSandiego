from pymongo import MongoClient
import pandas as pd

def fix():

    #Mongo setup 1982 week 2
    client = MongoClient('localhost', 27017)
    db = client.arch
##"PASSENGERS","ORIGIN","ORIGIN_CITY_NAME","ORIGIN_STATE_NM","DEST","DEST_CITY_NAME","DEST_STATE_NM","YEAR","MONTH",

    data = pd.DataFrame(list(db.fluData.find({"pneumonia_and_influenza_deaths":float('nan')})))

    ##mongo inserts
    for i in range(0, len(data)):
        fluRecord = {}
        fluRecord['_id'] = data.iloc[i]['_id']
        fluRecord['all_deaths'] = 0.0
        fluRecord['city'] = data.iloc[i]['city']
        fluRecord['region'] = data.iloc[i]['region']
        fluRecord['state'] = data.iloc[i]['state']
        fluRecord['week'] = data.iloc[i]['week']
        fluRecord['year'] = data.iloc[i]['year']
        fluRecord['pneumonia_and_influenza_deaths'] = 0.0
        fluRecord['density'] = 0.0
        print i
        db.fluData.update({'_id' : fluRecord['_id']},fluRecord, upsert=True)

if __name__ == '__main__':
    fix()
