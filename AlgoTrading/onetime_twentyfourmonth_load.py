import pymongo
import requests
import json
import datetime

from config.config import db_client, database_name

headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'authority':'api.bseindia.com','Sec-Ch-Ua':'Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                'Sec-Ch-Ua-Mobile':'0','Sec-Ch-Ua-Platform':'Windows"','Sec-Fetch-Dest':'empty','Sec-Fetch-Mode':'cors',
                'Sec-Fetch-Site':'same-site','Accept':'*/*','Accept-Encoding':'gzip, deflate, br',
                'Accept-Language':'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7','Origin':'https://www.bseindia.com',
                'Referer':'https://www.bseindia.com/'}

collection = pymongo.MongoClient(db_client)
ref_collection_name = "StockPriceDataTimeSeries"
dbcol = collection[database_name][ref_collection_name]
cursor = dbcol.find({})
data = [data["scripCode"] for data in cursor]
collection[database_name]["LastOneYearStockData"].delete_many({"scripName":"SBIN"})

failures = []

for i in ['500112']:
    try:
        baseapi = f"https://api.bseindia.com/BseIndiaAPI/api/StockReachGraph/w?scripcode={i}&flag=12M&fromdate=&todate=&seriesid="
        response = requests.get(baseapi,headers=headers)
        response_data = json.loads(response.content.decode())
        scriptname = response_data["Scripname"]
        response_data = response_data["Data"]
        response_data = json.loads(response_data)
        for index, data in enumerate(response_data):
            try:
                response_data[index]["scripName"] = scriptname
            except Exception as e:
                response_data[index]["scripName"] = f"Failed Scrip Name - {e} - {i}"
            try:
                response_data[index]["priceDate"] = datetime.datetime.strptime(response_data[index]["dttm"],"%a %b %d %Y %H:%M:%S")
            except Exception as e:
                response_data[index]["priceDate"] = datetime.datetime.now()
                response_data[index]["Exception"] = f"Exception in PriceDateConverstion + {e}"
            try:
                response_data[index]["Value"] = float(response_data[index]["vale1"])
            except Exception as e:
                response_data[index]["Value"] = 0.0
                response_data[index]["Exception"] = response_data[index]["Exception"]+f"Exception in Value + {e}"
            try:
                response_data[index]["Volume"] = float(response_data[index]["vole"])
            except Exception as e:
                response_data[index]["Volume"] = 0.0
                response_data[index]["Exception"] = response_data[index]["Exception"]+f"Exception in Value + {e}"
        x = collection[database_name]["LastOneYearStockData"].insert_many(response_data)
        print(response_data)
    except Exception as e:
        failures.append(i)
        with open("logfile.log", 'a') as f:
            f.write(f"Exception at {e} for {i}")
print(failures)

