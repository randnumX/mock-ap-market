import numpy as np
import pandas as pd
import requests
import xlsxwriter
import math
import bsedata.bse
import multiprocessing
import dask.dataframe as dd

stocks = pd.read_excel('bse_equities_reference.xlsx')
stocks.columns
stocks.sort_values()
bse_driver = bsedata.bse.BSE(update_codes = True)
his = bse_driver.getPeriodTrend('534976','6M')