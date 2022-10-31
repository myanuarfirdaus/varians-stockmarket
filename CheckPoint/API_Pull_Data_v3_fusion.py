
from alpha_vantage.alphavantage import AlphaVantage
from alpha_vantage.fundamentaldata import FundamentalData
from alpha_vantage.timeseries import TimeSeries
import numpy as np
import pandas as pd
from pandas import DataFrame as df, Timestamp
import csv
import ast
import requests
import random
import os

_API_KEY_TEST1 = 'G23MAAVKWB5TMPMV'
_API_KEY_TEST2 = '6PE06AMZAM1MCGFX' #myanuarfirdaus
_API_KEY_TEST3 = '6HZYCBP71FGOR1O5' #myanuarfirdaus23
_API_KEY_TEST4 = 'X5LNMI2AESR1YQCU' #muh_yanuar_firdaus@yahoo.com
_API_KEY_TEST5 = 'YBBKWRK4VSTP4GZH' #anggiengineer@yahoo.com

names=[_API_KEY_TEST1,_API_KEY_TEST2,_API_KEY_TEST3,_API_KEY_TEST4,_API_KEY_TEST5]
_API_KEY_TEST = ""
def randomize () :
    selected_names=set()
    global _API_KEY_TEST
    while len(selected_names)<1:
        random.shuffle(names)
        indx=random.randint(0,len(names)-1)
        selected_names = names[indx]
        _API_KEY_TEST = repr(selected_names)
        print (_API_KEY_TEST)
randomize()


#creating folder on C:


# define the name of the directory to be created
path = "/Varians"

try:
    os.makedirs(path)
except OSError:
    print ("Creation of the directory %s failed" % path)
else:
    print ("Successfully created the directory %s" % path)

#entity = tickername
entity= input ("Type stock market name here : ")

#call data API Fundamental Quarterly
fd = FundamentalData(key=_API_KEY_TEST, output_format='pandas')
# Get json object with the intraday data and another with  the call's metadata

data1, meta_data = fd.get_income_statement_quarterly(entity)  #get income statement quarterly data
randomize()
data2, meta_data = fd.get_cash_flow_quarterly(entity) #get cash flow quarterly data
randomize()
data3, meta_data = fd.get_balance_sheet_quarterly(entity) #get balance sheet quarterly data

API_URL = "https://www.alphavantage.co/query"
data = {
     "function": "OVERVIEW",
     "symbol": entity,
     "outputsize": "compact",
     "datatype": "json",
     "apikey": _API_KEY_TEST,
     }
response = requests.get(API_URL, data)
Company_Overview = response.json()


#call data timeseries last price monthly
randomize()
ts = TimeSeries(key=_API_KEY_TEST, output_format='pandas')
last_price, meta_data = ts.get_monthly(entity)

#write API to csv as buffer file
data1.to_csv(r'C:\Varians\Income Statement Quarterly.csv', index=None, header=True)
data2.to_csv(r'C:\Varians\Cash Flow Quarterly.csv', index=None, header=True)
data3.to_csv(r'C:\Varians\Balance Sheet Quarterly.csv', index=None, header=True)
last_price.to_csv(r'C:\Varians\Last Price.csv', index=True, header=True)

OV = {}
OV = Company_Overview
split_OV=OV

#write tuple/list overview to csv file
with open(r'C:\Varians\Overview2.csv', 'w') as g:
    writer1=csv.writer(g,lineterminator='\n')
    writer1.writerow(split_OV.keys())
    writer1.writerow(split_OV.values())

#read csv file to pull data for calculation variable
import pandas as pd
with open(r'C:\Varians\Income Statement Quarterly.csv') as ISQ:
    ISQ_reader = pd.read_csv(ISQ)
    
with open(r'C:\Varians\Cash Flow Quarterly.csv') as CFQ:
    CFQ_reader = pd.read_csv(CFQ)
    
with open(r'C:\Varians\Balance Sheet Quarterly.csv') as BSQ:
    BSQ_reader = pd.read_csv(BSQ)
    
with open(r'C:\Varians\Last Price.csv') as LP:
    LP_reader = pd.read_csv(LP)

#change date from object to datetime64
ISQ_reader.fiscalDateEnding=ISQ_reader.fiscalDateEnding.astype('datetime64')
ISQ_reader.replace(to_replace=['None'], value=np.nan, inplace=True) #to change string none to NaN
ISQ_reader.fillna(value=0,inplace=True)
CFQ_reader.fiscalDateEnding=CFQ_reader.fiscalDateEnding.astype('datetime64')
CFQ_reader.replace(to_replace=['None'], value=np.nan, inplace=True)
CFQ_reader.fillna(value=0,inplace=True)
BSQ_reader.fiscalDateEnding=BSQ_reader.fiscalDateEnding.astype('datetime64')
BSQ_reader.replace(to_replace=['None'], value=np.nan, inplace=True)
BSQ_reader.fillna(value=0,inplace=True)
BSQ_reader["commonStockSharesOutstanding"] = BSQ_reader.commonStockSharesOutstanding.astype(float) #to change  data type of SO
LP_reader.date=LP_reader.date.astype('datetime64')

cols = BSQ_reader.columns.drop('fiscalDateEnding')
BSQ_reader[cols] = BSQ_reader[cols].apply(pd.to_numeric, errors='coerce')
cols = ISQ_reader.columns.drop('fiscalDateEnding')
ISQ_reader[cols] = ISQ_reader[cols].apply(pd.to_numeric, errors='coerce')
cols = CFQ_reader.columns.drop('fiscalDateEnding')
CFQ_reader[cols] = CFQ_reader[cols].apply(pd.to_numeric, errors='coerce')

#make fundamental data from quarterly to monthly and fill forward NaN data with previous quarterly data
#this code only apply on int or float data types
#resample made the date ascending, while data from time series is descending. Why not just LP became ascending? Because LP has longer date than the other data
ISQ_monthly=ISQ_reader.resample('M', on='fiscalDateEnding').mean()
ISQ_monthly.fillna(method='ffill',inplace=True)
ISQ_monthly.sort_values(by=['fiscalDateEnding'], ascending=False, inplace=True)
CFQ_monthly=CFQ_reader.resample('M', on='fiscalDateEnding').mean()
CFQ_monthly.fillna(method='ffill',inplace=True)
CFQ_monthly.sort_values(by=['fiscalDateEnding'], ascending=False, inplace=True)
BSQ_monthly=BSQ_reader.resample('M', on='fiscalDateEnding').mean()
BSQ_monthly.fillna(method='ffill',inplace=True)
BSQ_monthly.sort_values(by=['fiscalDateEnding'], ascending=False, inplace=True)

#function for add rows to fundamental rows to be equal with timeseries rows and fill nan
def addrow(source,target,ori_table):
    data=[]
    #y=pd.DataFrame()
    counter=0
    for i in range(0,10):
        if source[i]==target[0]:
            break
        counter+=1
    for j in range(0,counter):
        data.insert(j, {np.NaN,np.NaN,np.NaN})
    y=pd.concat([pd.DataFrame(data), ori_table], ignore_index=True) #repeat for all variable
    y.fillna(method='bfill',inplace=True)
    return [y,counter]

#call all data needed for calculation and clean it first and wrap it into single variable list
ISQ_monthly.reset_index(inplace=True)
SO = BSQ_monthly['commonStockSharesOutstanding']
netIncome = ISQ_monthly['netIncome']
TA = BSQ_monthly['totalAssets']
TL = BSQ_monthly['totalLiabilities']
TSE = BSQ_monthly['totalShareholderEquity']
cash = BSQ_monthly['cash']
STD = BSQ_monthly['shortTermDebt']
LTD = BSQ_monthly['longTermDebt']
EBIT = ISQ_monthly['ebit']
DEPR = CFQ_monthly['depreciation']
AMOR = BSQ_monthly['accumulatedAmortization']

dict_table=pd.DataFrame(list(zip(SO,netIncome,TA,TL,TSE,cash,STD,LTD,EBIT,DEPR,AMOR)))
TS_table=LP_reader.loc[0:(len(netIncome)), ['date', '4. close']]

tempaddrow=addrow(TS_table.date,ISQ_monthly.fiscalDateEnding,dict_table) #use addrow function to add row to equal TS table row with quarter table row
datincome=pd.DataFrame(tempaddrow[0])
counter=tempaddrow[1]
allsindex=datincome.set_axis(['SO','netIncome','TA','TL','TSE','cash','STD','LTD','EBIT','DEPR','AMOR'], axis='columns')
TS_table=LP_reader.loc[0:(len(netIncome)+counter), ['date', '4. close']]
TS_table=TS_table.rename(columns={"4. close": "LastPrice"})
inputmerge=TS_table.merge(allsindex, left_index=True, right_index=True)
print(inputmerge)
#print(tempaddrow)

#function for making TTM data
def makettm(tabtemp,x):
    tabtemp = DataFrame(tabtemp)
    width = x
    shifted = tabtemp.shift(0)
    window = shifted.rolling(window=width)
    test = window.sum()
    complete = test.fillna(value=0)
    return complete

#calculating EPS TTM (Trailing Twelve Months) for PER input

#calculation for EPS Quarterly
from pandas import DataFrame
Tableeps =pd.DataFrame()
Tableeps ['Date'] = ISQ_reader.fiscalDateEnding
Tableeps ['NIQ'] = ISQ_reader.netIncome
Tableeps ['SOQ'] = BSQ_reader.commonStockSharesOutstanding
Tableeps = Tableeps.sort_index(ascending=False)
Tableeps = Tableeps.reset_index(drop=True)
EPStemps = [Tableeps.NIQ[i]/Tableeps.SOQ[i] for i in range(len(Tableeps.SOQ))]
NIQtemps = Tableeps.NIQ

#Make EPSTTM table from function makettm 
EPSTTM = makettm(EPStemps,4)
#EPSTTM = EPSTTM.rename(columns={'EPS':'EPSTTM'})
Tableeps ['EPSTTM'] = pd.DataFrame(EPSTTM)

#make netIncome TTM table from function makettm
NITTM = makettm(NIQtemps,4)
Tableeps['NITTM']=pd.DataFrame(NITTM)

Tableeps=Tableeps.resample('M', on='Date').mean()
Tableeps.fillna(method='ffill',inplace=True)
Tableeps = Tableeps.sort_index(ascending=False)
Tableeps.reset_index(inplace=True)
Coldate = Tableeps.Date
Tableeps = Tableeps.set_index('Date')
temp= addrow(TS_table.date,Coldate,Tableeps)
TTMmonth = temp[0]
print(TTMmonth)
print(Tableeps)
#gimana ngeluarin function result ke dataframe

#calculation
#columns=['EPS']
from pandas import DataFrame
Calculation = pd.DataFrame()

inputcalc = inputmerge
inputcalc = inputcalc.sort_index(ascending=False)
inputcalc = inputcalc.reset_index(drop=True)

#for i in range (len(SO)):
Calculation ['Date'] = inputcalc.date
Calculation ['LastPrice'] = inputcalc.LastPrice
Calculation ['ShareOut'] = inputcalc.SO
Calculation ['MarketCap'] = [inputcalc.LastPrice[i]*inputcalc.SO[i] for i in range(len(inputcalc.SO))]
Calculation ['EPS'] = [inputcalc.netIncome[i]/inputcalc.SO[i] for i in range(len(inputcalc.SO))]

#Merging TTM EPS
temp = pd.DataFrame()
temp['EPSTTM'] = (TTMmonth.EPSTTM)
temp['NITTM'] = TTMmonth.NITTM
temp = temp.sort_index(ascending=False)
temp.reset_index(inplace=True)
#Calculation = Calculation.merge(EPSTTM, left_index=True, right_index=True)

Calculation ['EPSTTM'] = temp.EPSTTM
Calculation ['PER'] = [inputcalc.LastPrice[i]/temp.EPSTTM[i] for i in range(len(inputcalc.SO))]
Calculation ['BVPS'] = [(inputcalc.TA[i]-inputcalc.TL[i])/inputcalc.SO[i] for i in range(len(inputcalc.SO))]
Calculation ['PBV'] = [Calculation.LastPrice[i]/Calculation.BVPS[i] for i in range(len(inputcalc.SO))]
Calculation ['ROA%'] = [temp.NITTM[i]/inputcalc.TA[i]*100 for i in range(len(inputcalc.SO))]
Calculation ['ROE%'] = [temp.NITTM[i]/inputcalc.TSE[i]*100 for i in range(len(inputcalc.SO))]
Calculation ['EV'] = [Calculation.MarketCap[i]+inputcalc.cash[i]-(inputcalc.STD[i]+inputcalc.LTD[i]) for i in range(len(inputcalc.SO))]
Calculation ['EBITDA'] = [inputcalc.EBIT[i]+inputcalc.DEPR[i]+inputcalc.AMOR[i] for i in range(len(inputcalc.SO))]
Calculation ['EV/EBITDA'] = [Calculation.EV[i]/Calculation.EBITDA[i] for i in range(len(inputcalc.SO))]
Calculation ['D/E'] = [inputcalc.TL[i]/inputcalc.TSE[i] for i in range(len(inputcalc.TL))]
Calculation ['Debt/Totalcap'] = [(inputcalc.STD[i]+inputcalc.LTD[i])/(inputcalc.STD[i]+inputcalc.LTD[i]+inputcalc.TSE[i]) for i in range(len(inputcalc.STD))]
Calculation ['Debt/EBITDA'] = [(inputcalc.STD[i]+inputcalc.LTD[i])/Calculation.EBITDA[i] for i in range(len(inputcalc.STD)) ]

Calculation = Calculation.replace([np.inf, -np.inf], 0)
print(Calculation.tail(30))
print(Calculation.dtypes)

Calculation.to_csv(r'C:\Varians\Calculation.csv', index=None, header=True)

#preparing pandas dataframe to list inputing to mysql

import time
mysql_list = []
Unique_ID  = []
StockName  = [entity.upper()]*len(Calculation['Date'])  

for i in range (len(Calculation['Date'])) :
    temp = entity + str(Calculation['Date'][i].strftime('%Y%m%d'))
    Unique_ID.append(temp)
    i += 1

mysql_list.append(Unique_ID)
mysql_list.append(StockName)

for i in list(Calculation.columns) :
    mysql_list.append(Calculation[i])
    
#print(mysql_list)
# data table overview

df_OV = pd.Series(OV, name='Info')
df_OV.index.name = 'Data'
list_header_OV = df_OV.index.tolist()

#import module for mysql 
import mplfinance as mpf
from os import path
import mysql.connector as mysql

def connect(db_name):
    try:
        return mysql.connect(
            host='localhost',
            user='root',
            password='minyak23',
            database=db_name)
    except Error as e:
        print(e)

if __name__ == '__main__':
    db = connect("newstockmarket")
    cursor = db.cursor()
    
    #--------------------------------------------------------------------------------------------------------------------------------------------------
    ftable = "stock_fundamental"
    ovtable = "stock_overview"
    fintable = "financial_report"
      
    create_ovtable = ("CREATE TABLE IF NOT EXISTS {table} "
                      #" ( Stock_ID int(5) NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                      " ( SYMBOL VARCHAR(8) NOT NULL PRIMARY KEY, ASSET_TYPE VARCHAR(30), NAME VARCHAR(20) NOT NULL, "
                      " DESCRIPTION VARCHAR(255), EXCHANGE VARCHAR(10), CURRENCY VARCHAR(5), "
                      " COUNTRY VARCHAR(50), SECTOR VARCHAR(50), INDUSTRY VARCHAR(50), "
                      " ADDRESS VARCHAR(255), Full_Time_Employees INT(20), Fiscal_Year_End VARCHAR(15), "
                      " LATEST_QUARTER DATE, DividendDate DATE, ExDividendDate DATE, "
                      " LastSplitFactor DECIMAL(5,2), LastSplitDate DATE )"
                     )
    create_fintable = ("CREATE TABLE IF NOT EXISTS {table} "
                      " ( NUMBER int(5) NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                      " SYMBOL VARCHAR(8), MARKET_CAP DECIMAL(15,2), EBITDA DECIMAL(15,2), "
                      " PER DECIMAL(15,2), PEGR DECIMAL(15,2), BOOK_VALUE DECIMAL(15,2), "
                      " Dividend_Per_Share DECIMAL(15,2), Dividend_Yield DECIMAL(15,2), EPS DECIMAL(15,2), "
                      " Revenue_Per_Share_TTM DECIMAL(15,2), Profit_Margin DECIMAL(15,2), Operating_Margin_TTM DECIMAL(15,2), "
                      " ROA_TTM DECIMAL(15,2), ROE_TTM DECIMAL(15,2), REVENUE_TTM DECIMAL(15,2), "
                      " Gross_Profit_TTM DECIMAL(15,2), Diluted_EPS_TTM DECIMAL(15,2), Quarterly_Earnings_Growth_YOY DECIMAL(15,2), "
                      " Quarterly_Revenue_Growth_YOY DECIMAL(15,2), Analyst_Target_Price DECIMAL(15,2), Trailing_PE DECIMAL(15,2), "
                      " Forward_PE DECIMAL(15,2), Price_to_Sales_Ratio_TTM DECIMAL(15,2), PBV DECIMAL(15,2), "
                      " EVtoRevenue DECIMAL(15,2), EVtoEBITDA DECIMAL(15,2), Beta DECIMAL(15,2), "
                      " 52WeekHigh DECIMAL(15,2), 52WeekLow DECIMAL(15,2), 50DayMovingAverage DECIMAL(15,2), "
                      " 200DayMovingAverage DECIMAL(15,2), SharesOutstanding DECIMAL(15,2), SharesFloat DECIMAL(15,2), " 
                      " SharesShort DECIMAL(15,2), SharesShortPriorMonth DECIMAL(15,2), ShortRatio DECIMAL(15,2), " 
                      " ShortPercentOutstanding DECIMAL(15,2), ShortPercentFloat DECIMAL(15,2), PercentInsiders DECIMAL(15,2), "
                      " PercentInstitutions DECIMAL(15,2), ForwardAnnualDividendRate DECIMAL(15,2), ForwardAnnualDividendYield DECIMAL(15,2), " 
                      " PayoutRatio DECIMAL(15,2) ) "
                      )
    create_ftable = ("CREATE TABLE IF NOT EXISTS {table} "
                   " ( UNIQUE_ID VARCHAR(20) NOT NULL PRIMARY KEY, SYMBOL VARCHAR(8) NOT NULL, "
                   " DATE date NOT NULL, LAST_PRICE_RP decimal(8, 2) NOT NULL,"
                   " SHARE_OUT decimal(15, 2) NOT NULL, MARKET_CAP_RP decimal(15, 2) NOT NULL, "
                   " DEVIDEN_RP decimal(15, 2) NOT NULL,  EPSTTMM_RP decimal(15, 2) NOT NULL, "
                   " PER_X decimal(15, 2) NOT NULL, BVPS_RP decimal(15, 2) NOT NULL, "
                   " PBV_X decimal(15, 2) NOT NULL, ROA_PERCENT decimal(15, 2) NOT NULL, "
                   " ROE_PERCENT decimal(15, 2) NOT NULL, EV decimal(15, 2) NOT NULL, "
                   " EBITDA decimal(15, 2) NOT NULL, EV_EBITDA_RATIO decimal(15, 2) NOT NULL, "
                   " D_E_RATIO decimal(15, 2) NOT NULL, DEBT_TOTALCAP_RATIO decimal(15, 2) NOT NULL, "
                   " DEBT_EBITDA_RATIO decimal(15, 2) NOT NULL) "
                   )

    cursor = db.cursor()
    cursor.execute(create_ovtable.format(table=ovtable) )                    
    cursor.execute(create_fintable.format(table=fintable, atable=fintable) )                
    cursor.execute(create_ftable.format(table=ftable, atable=ftable) )

#------------------------------------------------------------------------------------------
    insert_ovtable = ("INSERT IGNORE INTO {table} "
                    "( SYMBOL, ASSET_TYPE, "
                    "NAME, DESCRIPTION, EXCHANGE, "
                    "CURRENCY, COUNTRY, SECTOR, "
                    "INDUSTRY, ADDRESS, Full_Time_Employees, "
                    "Fiscal_Year_End, LATEST_QUARTER, DividendDate, "
                    "ExDividendDate, LastSplitFactor, LastSplitDate ) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                     )
    temp_data = []
    for i in range(13) :
        single_data = df_OV[list_header_OV[i]]
        temp_data.append(single_data)
    for i in range(55, len(list_header_OV)) :
        single_data = df_OV[list_header_OV[i]]
        temp_data.append(single_data)
    cursor.execute(insert_ovtable.format(table=ovtable), temp_data)
    
    insert_fintable = ("INSERT IGNORE INTO {table} "
                        "( SYMBOL, MARKET_CAP, EBITDA, " 
                        " PER, PEGR, BOOK_VALUE, "
                        " Dividend_Per_Share, Dividend_Yield, EPS, "
                        " Revenue_Per_Share_TTM, Profit_Margin, Operating_Margin_TTM, "
                        " ROA_TTM, ROE_TTM, REVENUE_TTM, "
                        " Gross_Profit_TTM, Diluted_EPS_TTM, Quarterly_Earnings_Growth_YOY, "
                        " Quarterly_Revenue_Growth_YOY, Analyst_Target_Price, Trailing_PE, "
                        " Forward_PE, Price_to_Sales_Ratio_TTM, PBV, "
                        " EVtoRevenue, EVtoEBITDA, Beta, "
                        " 52WeekHigh, 52WeekLow, 50DayMovingAverage, "
                        " 200DayMovingAverage, SharesOutstanding, SharesFloat, "
                        " SharesShort, SharesShortPriorMonth, ShortRatio, "
                        " ShortPercentOutstanding, ShortPercentFloat, PercentInsiders, "
                        " PercentInstitutions, ForwardAnnualDividendRate, ForwardAnnualDividendYield, PayoutRatio ) "
                        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                        " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                        " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
                      )
    temp_data = []
    temp_data.append(entity)
    for i in range(13, 55) :
        single_data = df_OV[list_header_OV[i]]
        temp_data.append(single_data)
    cursor.execute(insert_fintable.format(table=fintable), temp_data)

    insert_ftable = ("INSERT IGNORE INTO {table} "
                   "(UNIQUE_ID, SYMBOL, DATE, LAST_PRICE_RP, SHARE_OUT, MARKET_CAP_RP, "
                   "DEVIDEN_RP, EPSTTMM_RP, PER_X, BVPS_RP, PBV_X, ROA_PERCENT, ROE_PERCENT, "
                   "EV, EBITDA, EV_EBITDA_RATIO, D_E_RATIO, DEBT_TOTALCAP_RATIO, "
                   "DEBT_EBITDA_RATIO) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    j = 0
    while j < len(mysql_list[0]) :
        temp_data = []
        i = 0
        while i < len(mysql_list) :
            temp_data.append(str (mysql_list[i][j]))
            #print(project_data)
            i += 1
        
        cursor.execute(insert_ftable.format(table=ftable), temp_data)
        j += 1
        
    db.commit()

    #select_table = ("SELECT * FROM {table} ")
    #cursor.execute(select_table.format(table=ftable))
    #project_records = cursor.fetchall()
    
    #print(project_records)

    db.close()