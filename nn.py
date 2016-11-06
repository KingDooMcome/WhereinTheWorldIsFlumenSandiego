from pymongo import MongoClient
import pandas as pd
import numpy as np
import hashlib
from numpy import ravel
from sklearn.metrics import explained_variance_score,r2_score
from sklearn.neighbors import KNeighborsRegressor
from sklearn import svm
def nn():
    abbr={'AL': "Alabama" ,'AK' :"Alaska" ,'AZ': "Arizona" ,'AR':"Arkansas",'CA': "California",'CO': "Colorado",'CT':"Connecticut",'DE':"Delaware",
            'FL':"Florida",'GA':"Georgia",'HI': "Hawaii",'ID':"Idaho",'IL': "Illinois",'IN': "Indiana",'IA': "Iowa",'KS': "Kansas",
            'KY': "Kentucky",'LA': "Louisiana",'ME': "Maine",'MD': "Maryland",'MA': "Massachusetts",'MI': "Michigan",'MN': "Minnesota",'MS': "Mississippi",
            'MO': "Missouri",'MT': "Montana" ,'NE': "Nebraska",'NV': "Nevada",'NH': "New Hampshire",'NJ': "New Jersey",'NM': "New Mexico" ,'NY': "New York",
            'NC': "North Carolina",'ND': "North Dakota",'OH': "Ohio",'OK': "Oklahoma",'OR': "Oregon",'PA': "Pennsylvania",'RI': "Rhode Island",'SC': "South Carolina",
            'SD': "South Dakota",'TN': "Tennessee",'TX':"Texas",'UT': "Utah",'VT': "Vermont" ,'VA': "Virginia",'WA': "Washington",'WV': "West Virginia",
            'WI': "Wisconsin",'WY': "Wyoming"}
    stat = {"Alabama": 0, "Alaska": 1, "Arizona": 2, "Arkansas": 3, "California": 4, "Colorado": 5,
            "Connecticut": 6, "Delaware": 7,
            "Florida": 8, "Georgia": 9, "Hawaii":10, "Idaho":11, "Illinois":12, "Indiana":13, "Iowa":14,
            "Kansas":15,
            "Kentucky":16, "Louisiana":17, "Maine":18, "Maryland":19, "Massachusetts":20, "Michigan":21,
            "Minnesota":22, "Mississippi":23,
            "Missouri":24, "Montana":25, "Nebraska":26, "Nevada":27, "New Hampshire":28, "New Jersey":29,
            "New Mexico":30, "New York":31,
            "North Carolina":32, "North Dakota":33, "Ohio":34, "Oklahoma":35, "Oregon":36, "Pennsylvania":37,
            "Rhode Island":38, "South Carolina":39,
            "South Dakota":40, "Tennessee":41, "Texas":42, "Utah":43, "Vermont":44, "Virginia":45,
            "Washington":46, "West Virginia":47,
            "Wisconsin":48, "Wyoming":49}
    #Mongo setup 1982 week 2
    client = MongoClient('localhost', 27017)
    db = client.arch
    df_train = pd.DataFrame(list(db.fluData.find({"year": {"$lt": 2013, "$gt": 1989}})))
    df_test = pd.DataFrame(list(db.fluData.find({"year": {"$lt": 2016, "$gt": 2012}})))
    df_train = df_train.dropna(how='any', axis=0)
    df_test = df_test.dropna(how='any', axis=0)

    for i, row in df_train.iterrows():
        df_train.set_value(i, 'state', stat[abbr[row['state']]])
        df_train.set_value(i, 'city', abs(hash(row['city'])) % (10 ** 8))
    for i, row in df_test.iterrows():
        df_test.set_value(i, 'state', stat[abbr[row['state']]])
        df_test.set_value(i, 'city', abs(hash(row['city'])) % (10 ** 8))
    print "Done parsing"

    
    neighDeath = KNeighborsRegressor(n_neighbors=3 )
    neighDeath.fit(df_train[['city','state', 'region', 'unemployment', 'week', 'year']],(df_train[['all_deaths']]))
    predDeath = neighDeath.predict(df_test[['city','state', 'region', 'unemployment', 'week', 'year']])

    neighFlu = KNeighborsRegressor(n_neighbors=6,weights='distance')
    neighFlu.fit(df_train[['city','state', 'region', 'unemployment', 'all_deaths','week', 'year']],
                 df_train[['pneumonia_and_influenza_deaths']])
    predFlu = neighFlu.predict(df_test[['city','state', 'region', 'unemployment', 'all_deaths','week', 'year']])


    print "PREDICTED"
    print "Eval: fluDeath"

    print explained_variance_score(df_test[['pneumonia_and_influenza_deaths']], predFlu)
    print explained_variance_score(df_test[['all_deaths']], predDeath)

    print r2_score(df_test[['pneumonia_and_influenza_deaths']], predFlu)
    print r2_score(df_test[['all_deaths']], predDeath)

    for year in range(2016,2021):
        for week in range(1,53):
            data =pd.DataFrame(list(db.fluData.find({"year":2014,"week":2})))
            data.fillna(0, axis=0)
            for i in range(len(data)):
                fluRecord = {}
                fluRecord['city'] = data.iloc[i]['city']
                fluRecord['region'] = data.iloc[i]['region']
                fluRecord['state'] = data.iloc[i]['state']
                fluRecord['unemployment'] = data.iloc[i]['unemployment']
                fluRecord['week'] = week
                fluRecord['year'] = year
                data.set_value(i, 'state', stat[abbr[row['state']]])
                data.set_value(i, 'city', abs(hash(row['city'])) % (10 ** 8))
                try:
                    fluRecord['all_deaths'] = int(neighDeath.predict([data.iloc[i]['city'], data.iloc[i]['state'],fluRecord['region'],fluRecord['unemployment'], week,year]).item(0))
                except ValueError:
                    fluRecord['all_deaths'] =0
                try:
                    fluRecord['pneumonia_and_influenza_deaths'] = int(neighFlu.predict([data.iloc[i]['city'], data.iloc[i]['state'], fluRecord['region'], fluRecord['unemployment'], fluRecord['all_deaths'], week, year]).item(0))
                except ValueError:
                    fluRecord['pneumonia_and_influenza_deaths'] =0
                fluRecord['density'] = float(data.iloc[i]['pneumonia_and_influenza_deaths']) / (
                float(data.iloc[i]['all_deaths']) + .0000001)
                print i
                db.fluData.insert(fluRecord)

if __name__ == '__main__':
    nn()