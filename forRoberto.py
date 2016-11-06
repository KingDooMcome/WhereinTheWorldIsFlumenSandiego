from pymongo import MongoClient
import pandas as pd

def loadAir():

    #Mongo setup 1982 week 2
    client = MongoClient('localhost', 27017)
    db = client.arch
##"PASSENGERS","ORIGIN","ORIGIN_CITY_NAME","ORIGIN_STATE_NM","DEST","DEST_CITY_NAME","DEST_STATE_NM","YEAR","MONTH",
    for year in range(2016, 2021):
        for week in range(1, 53):
            data = pd.DataFrame(list(db.fluData.find({"year": year, "week": week})))
            print len(data)
            f = open(''+str(year)+str(week)+'.js', 'w')
            f.write("var test = [")
            for i in range(0, len(data)):
                f.write('"<b>'+str(data.iloc[i]['city'])+'</b><br>Deaths: '+str(data.iloc[i]['density'])+"<br>Jobs: "+str(data.iloc[i]['unemployment'])+'"')
                if i!=len(data)-1:
                    f.write(',')
            f.write("];")
            f.close()

if __name__ == '__main__':
    loadAir()