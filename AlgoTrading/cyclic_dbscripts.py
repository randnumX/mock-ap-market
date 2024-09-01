import time
import pymongo
import bsedata.bse
import datetime
import multiprocessing
from functools import partial
from config.config import db_client,database_name

def true_value(value):
    if type(value)==str:
        if 'lakh' in value.lower():
            return float(value.lower().replace('lakh', '').replace(" ", "").replace(",","")) * 100000
        elif 'cr.'  in value.lower():
            return float(value.lower().replace('cr.', '').replace(" ", "").replace(",","")) * 10000000
    return value
def get_data(data,bse_driver,collection_name,db_client,database_name):
    try:
        temp_response = bse_driver.getQuote(f'{data}')
        temp_response['updatedOn'] = datetime.datetime.strptime(temp_response['updatedOn'], "%d %b %y | %I:%M %p")
        for i in temp_response:
            if i in ["currentValue","change","pChange","faceValue","previousClose","previousOpen","dayHigh","dayLow",
                     "52weekHigh","52weekLow","weightedAvgPrice"]:
                temp_response[i] = float(temp_response[i])
            elif i in ["totalTradedValue","totalTradedQuantity","2WeekAvgQuantity","marketCapFull",
                       "marketCapFreeFloat"]:
                temp_response[i] = true_value(temp_response[i])
        collection = pymongo.MongoClient(db_client)
        print(temp_response)
        collection[database_name][collection_name].insert_one(temp_response)
    except Exception as e:
        return f"{data} -- {e}"
    return "Success"

if __name__=='__main__':
    starttime = time.time()
    db_client = db_client
    database_name = database_name
    collection_name = "StockPriceDataTimeSeries"
    ref_collection_name = "StockPricesData"
    collection = pymongo.MongoClient(db_client)
    bse_driver = bsedata.bse.BSE(update_codes=True)
    cursor = collection[database_name][ref_collection_name].find({})
    data = [data["Security Code"] for data in cursor]
    with multiprocessing.Pool(processes=60) as pool:
        results = pool.map(partial(get_data, bse_driver=bse_driver,collection_name=collection_name,db_client=db_client,database_name=database_name),data)
    pool.close()
    print(results)
    print(f"Here we are ... and the process took {time.time()-starttime} Seconds!")

    # #Non-Parallel
    # data = [data["Security Code"] for data in cursor][0]
    # get_data(data,bse_driver,collection_name,db_client,database_name)


