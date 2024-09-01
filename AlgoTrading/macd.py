import math

import pymongo
from config.config import *
import pandas as pd
import matplotlib.pyplot as plt

import warnings
warnings.filterwarnings("ignore")

shorterdays= 12
longerdays = 26
selling_percentage = 10
noofholdingdays = 5

def macd(indicator,connection,money_given):
    cursor = connection[past24month_collection].find({"scripName":indicator})
    data = [data for data in cursor]
    df = pd.DataFrame(data)
    df.sort_values(by=["priceDate"],inplace=True)
    df["shorter_data_ema"] = 0
    df["longer_data_ema"] = 0
    df["macd_value"] = 0
    df["signal"] = 0
    initial_money = money_given
    profit = 0

    # print(df.columns)
    for i,row in df.iterrows():
        if i>shorterdays:
            df.at[i,"shorter_data_ema"] = row["Value"]*(2/(shorterdays+1)) +( df.at[i-1,"shorter_data_ema"]*(1-(2/(1+shorterdays))))
        else:
            df.at[i, "shorter_data_ema"] = df[:i]["Value"].mean()
            df.at[i, "signal"] = df.iloc[(i - 9):i]['macd_value'].mean()
        if i > longerdays:
            df.at[i,"longer_data_ema"] = row["Value"]*(2/(longerdays+1)) + (df.at[i-1,"longer_data_ema"]*(1-(2/(1+longerdays))))
            df.at[i, "macd_value"] = df.at[i, "shorter_data_ema"] - df.at[i, "longer_data_ema"]
            df.at[i, "signal"] = df.at[i, "macd_value"]*(2/(9+1)) + (df.at[i-1,"signal"]*(1-(2/(1+9)))) #df.iloc[(i - 9):i]['macd_value'].mean()
        else:
            df.at[i, "longer_data_ema"] = df[:i]["Value"].mean()

    bought = False
    for i,row in df[-10:].iterrows():
        if i>30:
            if df.at[i,"macd_value"]>df.at[i,"signal"]  and df.at[i-1,"macd_value"]<df.at[i-1,"signal"] and df.at[i,"macd_value"]<0:
                print(f'MACD - {df.at[i-1,"macd_value"]}-- SIGNAL - {df.at[i-1,"signal"]}')
                print(f'MACD - {df.at[i,"macd_value"]}-- SIGNAL - {df.at[i,"signal"]}')
                print(f"Bought at - {df.at[i,'Value']} on {df.at[i,'priceDate']}")
                with open("buysell.log",'a') as file:
                    file.write(f"Bought {indicator} at - {df.at[i,'Value']} on {df.at[i,'priceDate']}\n")
                bought = True
                boughtivalue = i
                boughtat =df.at[i,'Value']
                number_of_stocks_bought = math.floor(money_given/boughtat)
                money_given = money_given - number_of_stocks_bought*boughtat - 5
                # print(money_given)
            boughtat,number_of_stocks_bought =0, 0
            #
            # or (df.at[i,'Value']-boughtat/boughtat<=-selling_percentage/100) or (i-boughtivalue>=noofholdingdays) or (df.at[i,'Value']-boughtat)/boughtat>=selling_percentage/100)
            if (df.at[i,"macd_value"]<df.at[i,"signal"] and df.at[i-1,"macd_value"]>df.at[i-1,"signal"]) and df.at[i,"macd_value"]>0:
                print(f'MACD - {df.at[i-1,"macd_value"]}-- SIGNAL - {df.at[i-1,"signal"]}')
                print(f'MACD - {df.at[i,"macd_value"]}-- SIGNAL - {df.at[i,"signal"]}')
                print(f"Sold at - {df.at[i,'Value']} on {df.at[i,'priceDate']}")
                bought = False
                money_given = money_given + df.at[i,'Value']*number_of_stocks_bought - 5
                # print(money_given)
                localprofit = df.at[i,'Value'] - boughtat -5
                profit = profit+localprofit
                print(f"Profit = {df.at[i,'Value'] -boughtat}")
                with open("buysell.log",'a') as file:
                    file.write(f"Sold {indicator} at - {df.at[i,'Value']} on {df.at[i,'priceDate']}\n")
    print(f"Final Profit {indicator} = {initial_money} -> {money_given} - ROI - {(money_given - initial_money) * 100 / initial_money}%")

    # df.index = df["priceDate"]
    # df[["macd_value","signal"]].plot(title="DataFrame Plot")
    # df[["Value"]].plot(title="DataFrame Plot")
    plt.show()
if __name__=='__main__':
    connection = pymongo.MongoClient(db_client)[database_name]
    money = int(input("Enter the number of money"))
    for i in pymongo.MongoClient(db_client)[database_name][collection_name].find({}):
        try:
            macd(i["Security Id"],connection,money)
        except Exception as e:
            with open("error.log",'a') as file:
                    file.write(f"Exception -  {i} at - {e}\n")

    import os
    os.system("shutdown /s /t 1")