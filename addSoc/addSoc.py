from pymongo import MongoClient
import pandas as pd
import os

def addSoc():

    #Mongo setup 1982 week 2
    client = MongoClient('localhost', 27017)
    db = client.arch

    fileList = []
    files = os.listdir(".")
    for i in files:
        if i.endswith('.csv'):
            fileList.append(i)
    csv = pd.DataFrame()
    ##mongo inserts
    for k in range(len(fileList)):
        file = fileList[k]
        print file
        soc = pd.read_csv(file)
        un = {}
        for i in range(len(soc)):
            un[soc.iloc[i]['Year']]= 0

        for i in range(len(soc)):
            un[soc.iloc[i]['Year']] += float(soc.iloc[i]['Value'])

        for yea in range(1990,2016):
            data = pd.DataFrame(list(db.fluData.find({ "year":yea, "city": str(file).replace('.csv', '')})))
            for i in range(0, len(data)):
                fluRecord = {}
                fluRecord['_id'] = data.iloc[i]['_id']
                fluRecord['all_deaths'] = data.iloc[i]['all_deaths']
                fluRecord['city'] = data.iloc[i]['city']
                fluRecord['region'] = data.iloc[i]['region']
                fluRecord['state'] = data.iloc[i]['state']
                fluRecord['week'] = data.iloc[i]['week']
                fluRecord['year'] = data.iloc[i]['year']
                fluRecord['pneumonia_and_influenza_deaths'] = data.iloc[i]['pneumonia_and_influenza_deaths']
                fluRecord['density'] = data.iloc[i]['density']
                fluRecord['unemployment'] = un[data.iloc[i]['year']]/12
                print fluRecord
                db.fluData.update({'_id': fluRecord['_id']}, fluRecord, upsert=True)



if __name__ == '__main__':
    addSoc()
