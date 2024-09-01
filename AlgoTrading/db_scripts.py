import pymongo
import pandas as pd
from config.config import db_client,bse_equities_reference,database_name
import json

db = ""
db.users.aggregate([
    {
        '$lookup':{
            'from': "userinfo",       #other table name
            'localField': "userId",   #name of users table field
            'foreignField': "userId", # name of userinfo table field
            'as': "user_info"         #alias for userinfo table
        }
    },
    {   '$unwind':"$user_info" },     # $unwind used for getting data in object or for one record only

    /# Join with user_role table
    {
        '$lookup':{
            'from': "userrole",
            'localField': "userId",
            'foreignField': "userId",
            'as': "user_role"
        }
    },
    {   '$unwind':"$user_role" },

    #  define some conditions here
    {
        '$match':{
            '$and':[{"userName" : "admin"}]
        }
    },

    #define which fields are you want to fetch
    {
        '$project':{
            '_id' : 1,
            'email' : 1,
            'userName' : 1,
            'userPhone' : "$user_info.phone",
            'role' : "$user_role.role",
        }
    }
])
def create_db_connection_collection():
    print("============CONNECTION STARTED=================")
    connection = pymongo.MongoClient(db_client)
    db = connection[database_name]
    print("============CONNECTION COMPLETED=================")
    return db

def delete_all_data_of_collection(collection,collection_name):
    print("============DELETION OF EXIXSTING FILES STARTED=================")
    collection[collection_name].delete_many({})
    print("============DELETION OF EXIXSTING FILES COMPELTED=================")

def del_and_initate_all_ticker_data(collection,collection_name):
    print("============INSERTION FILES STARTED=================")
    data = pd.read_excel(bse_equities_reference)
    delete_all_data_of_collection(collection,collection_name)
    collection[collection_name].insert_many(json.loads(data.to_json(orient='records')))
    print("============INSERTION FILES COMPLETED=================")

if __name__=='__main__':
    print("Hello")

