from alpha_vantage.fundamentaldata import FundamentalData
from alpha_vantage.timeseries import TimeSeries
import numpy as np
import pandas as pd
from pandas import DataFrame as df, Timestamp
import csv
import time
#import module for mysql 
import os
from os import path
import mysql.connector as mysql
from CalFundamental import calFund
from APICall import ApiCalling, ApiKeyDistribute
from mysqlProc import createTableSQL, insertTableSQL


def makeFolder() :
    # define the name of the directory to be created
    path = "/Varians"
    #creating folder:
    try:
        os.makedirs(path)
    except OSError:
        print ("Directory %s already exist" % path)
    else:
        print ("Successfully created the directory %s" % path)

def stocklisting() :
    with open(r'stocklist.csv') as slist:
        stocklist = pd.read_csv(slist, header = None)    
    stocklist = stocklist.rename(columns={0 : 'company', 1 : 'market', 2 : 'ticker'})
    stocklist.ticker = stocklist.ticker.str.strip()
    return stocklist.ticker

if __name__ == "__main__":        
    #check and create folder varians as temporary storage
    serverHost = "remotemysql.com"
    userInput = "0NMKbNP49w"
    mysqlPassword = "uh7eChPxHw"
    db_name = "0NMKbNP49w"

    API_KEY_TEST = ""
    makeFolder()
    stocklist = stocklisting()
    
    #creating table in mysql database, insert password accordingly
    createTableSQL(serverHost, userInput, mysqlPassword, db_name)
    
    for zticker in list (stocklist) :
        entity = str(zticker)
        print ("Stock Ticker Name : "+ entity)
    
        #reset data memory
        OV_reader = pd.DataFrame()
        ISQ_reader = pd.DataFrame()
        CFQ_reader = pd.DataFrame()
        BSQ_reader = pd.DataFrame()
        dict_table = pd.DataFrame()

        #API calling
        API_KEY_TEST = ApiKeyDistribute().randomize(API_KEY_TEST)
        e = ApiCalling(zticker, API_KEY_TEST).step_1()
        ApiCalling(zticker, API_KEY_TEST).step_2(e)
        ApiCalling(zticker, API_KEY_TEST).step_3(e)
        ApiCalling(zticker, API_KEY_TEST).step_4(e)
        ApiCalling(zticker, API_KEY_TEST).step_5(e)

        if e != 'Skip Ticker' :
            try :
                Calculation, CFQ_reader, ISQ_reader, BSQ_reader, OV_reader = calFund()
                #preparing pandas dataframe to list inputing to mysql
                insertTableSQL(serverHost, userInput, mysqlPassword, db_name, entity, Calculation, CFQ_reader, ISQ_reader, BSQ_reader, OV_reader)
            except TypeError as e :
                continue