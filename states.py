from pymongo import MongoClient
import pandas as pd
import re
from shutil import copyfile

def states():
    # Mongo setup 1982 week 2
    client = MongoClient('localhost', 27017)
    db = client.arch

    abbr={'AL': "Alabama" ,'AK' :"Alaska" ,'AZ': "Arizona" ,'AR':"Arkansas",'CA': "California",'CO': "Colorado",'CT':"Connecticut",'DE':"Delaware",
            'FL':"Florida",'GA':"Georgia",'HI': "Hawaii",'ID':"Idaho",'IL': "Illinois",'IN': "Indiana",'IA': "Iowa",'KS': "Kansas",
            'KY': "Kentucky",'LA': "Louisiana",'ME': "Maine",'MD': "Maryland",'MA': "Massachusetts",'MI': "Michigan",'MN': "Minnesota",'MS': "Mississippi",
            'MO': "Missouri",'MT': "Montana" ,'NE': "Nebraska",'NV': "Nevada",'NH': "New Hampshire",'NJ': "New Jersey",'NM': "New Mexico" ,'NY': "New York",
            'NC': "North Carolina",'ND': "North Dakota",'OH': "Ohio",'OK': "Oklahoma",'OR': "Oregon",'PA': "Pennsylvania",'RI': "Rhode Island",'SC': "South Carolina",
            'SD': "South Dakota",'TN': "Tennessee",'TX':"Texas",'UT': "Utah",'VT': "Vermont" ,'VA': "Virginia",'WA': "Washington",'WV': "West Virginia",
            'WI': "Wisconsin",'WY': "Wyoming"}
    for year in range(2016, 2021):
        for week in range(1, 53):
            print week
            stat = {"Alabama": 0, "Alaska": 0, "Arizona": 0, "Arkansas": 0, "California": 0, "Colorado": 0,
                    "Connecticut": 0, "Delaware": 0,
                    "Florida": 0, "Georgia": 0, "Hawaii": 0, "Idaho": 0, "Illinois": 0, "Indiana": 0, "Iowa": 0,
                    "Kansas": 0,
                    "Kentucky": 0, "Louisiana": 0, "Maine": 0, "Maryland": 0, "Massachusetts": 0, "Michigan": 0,
                    "Minnesota": 0, "Mississippi": 0,
                    "Missouri": 0, "Montana": 0, "Nebraska": 0, "Nevada": 0, "New Hampshire": 0, "New Jersey": 0,
                    "New Mexico": 0, "New York": 0,
                    "North Carolina": 0, "North Dakota": 0, "Ohio": 0, "Oklahoma": 0, "Oregon": 0, "Pennsylvania": 0,
                    "Rhode Island": 0, "South Carolina": 0,
                    "South Dakota": 0, "Tennessee": 0, "Texas": 0, "Utah": 0, "Vermont": 0, "Virginia": 0,
                    "Washington": 0, "West Virginia": 0,
                    "Wisconsin": 0, "Wyoming": 0}
            count = {"Alabama": 0, "Alaska": 0, "Arizona": 0, "Arkansas": 0, "California": 0, "Colorado": 0,
                     "Connecticut": 0, "Delaware": 0,
                     "Florida": 0, "Georgia": 0, "Hawaii": 0, "Idaho": 0, "Illinois": 0, "Indiana": 0, "Iowa": 0,
                     "Kansas": 0,
                     "Kentucky": 0, "Louisiana": 0, "Maine": 0, "Maryland": 0, "Massachusetts": 0, "Michigan": 0,
                     "Minnesota": 0, "Mississippi": 0,
                     "Missouri": 0, "Montana": 0, "Nebraska": 0, "Nevada": 0, "New Hampshire": 0, "New Jersey": 0,
                     "New Mexico": 0, "New York": 0,
                     "North Carolina": 0, "North Dakota": 0, "Ohio": 0, "Oklahoma": 0, "Oregon": 0, "Pennsylvania": 0,
                     "Rhode Island": 0, "South Carolina": 0,
                     "South Dakota": 0, "Tennessee": 0, "Texas": 0, "Utah": 0, "Vermont": 0, "Virginia": 0,
                     "Washington": 0, "West Virginia": 0,
                     "Wisconsin": 0, "Wyoming": 0}
            data = pd.DataFrame(list(db.fluData.find({"year": year, "week": week})))
            data.fillna(value=0, axis=0)
            for i in range(0, len(data)):
                stat[abbr[data.iloc[i]['state']]] += data.iloc[i]['density']
                count[abbr[data.iloc[i]['state']]]+=1
            for i in stat:
                stat[i] /= count[i]+0.0000000000000001
            copyfile('states.json', '' + str(year) + str(week) + '.js')
            f = open('states.json', 'r')
            js=''
            js += (f.read())
            f.close()
            for s in stat:
                js = re.sub('"'+s+'","density":\d*.\d*}', '"'+s+'","density":'+str(stat[s])+'}', js)
            f = open('' + str(year) + str(week) + '.js', 'w')
            f.write("var c ="+js+";")
            f.close()

if __name__ == '__main__':
    states()