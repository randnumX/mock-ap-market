from db_scripts import create_db_connection_collection, del_and_initate_all_ticker_data,delete_all_data_of_collection
from config.config import collection_name,timeseries_collection


if __name__=='__main__':
    try:
        print("===============Application Has Started==============")
        connection = create_db_connection_collection()
        #reset the data for the stock Market based on the new excel file in folder base excel
        while(True):
            selection = int(input("Select The Option.\n1.Initiate the data from the excel\n2.Delete all the Ticker Data\n3.Delete all the Time Series Data\n"))
            if selection== 1:
                del_and_initate_all_ticker_data(connection,collection_name)
            elif selection == 2:
                delete_all_data_of_collection(connection,collection_name)
            elif selection == 3:
                delete_all_data_of_collection(connection,timeseries_collection)
    except Exception:
        print("===============Application Has Ended==============")