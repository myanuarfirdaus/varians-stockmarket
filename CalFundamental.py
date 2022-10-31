def calFund():
    import numpy as np
    import pandas as pd
    from pandas import DataFrame as df, Timestamp
    import csv
    import requests
    import os
    import time
    #import module for mysql 
    from os import path
    import mysql.connector as mysql
    from pandas import DataFrame
    from TTM import addrow, makettm

    with open(r'C:\Varians\Overview.csv') as OV:
        OV_reader = pd.read_csv(OV, index_col=False, header=0)
    OV_reader = OV_reader.loc[0,:]   
    
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

    #call all data needed for calculation and clean it first and wrap it into single variable list
    ISQ_monthly.reset_index(inplace=True)
    SO = BSQ_monthly['commonStockSharesOutstanding']
    netIncome = ISQ_monthly['netIncome']
    TA = BSQ_monthly['totalAssets']
    TL = BSQ_monthly['totalLiabilities']
    TSE = BSQ_monthly['totalShareholderEquity']
    cash = BSQ_monthly['cashAndCashEquivalentsAtCarryingValue']
    STD = BSQ_monthly['shortTermDebt']
    LTD = BSQ_monthly['longTermDebt']
    EBIT = ISQ_monthly['ebit']
    DEPR = CFQ_monthly['depreciationDepletionAndAmortization']
    #AMOR = BSQ_monthly['accumulatedAmortization']

    dict_table=pd.DataFrame(list(zip(SO,netIncome,TA,TL,TSE,cash,STD,LTD,EBIT,DEPR)))
    TS_table=LP_reader.loc[0:(len(netIncome)), ['date', '5. adjusted close']]

    tempaddrow=addrow(TS_table.date,ISQ_monthly.fiscalDateEnding,dict_table) #use addrow function to add row to equal TS table row with quarter table row
    datincome=pd.DataFrame(tempaddrow[0])
    counter=tempaddrow[1]


    allsindex=datincome.rename(columns={ 0 : 'SO', 1 : 'netIncome', 2 : 'TA', 3 : 'TL', 4 : 'TSE', 5 : 'cash', 6 : 'STD', 7 : 'LTD', 8 : 'EBIT', 9 : 'DEPR' })
    TS_table=LP_reader.loc[0:(len(netIncome)+counter), ['date', '5. adjusted close']]
    TS_table=TS_table.rename(columns={"5. adjusted close": "LastPrice"})
    inputmerge=TS_table.merge(allsindex, left_index=True, right_index=True)
    #print(inputmerge)
    #print(tempaddrow)

    #calculating EPS TTM (Trailing Twelve Months) for PER input

    #calculation for EPS Quarterly

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
    #print(TTMmonth)
    #print(Tableeps)
    #gimana ngeluarin function result ke dataframe

    #calculation
    #columns=['EPS']

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
    Calculation ['EBITDA'] = [inputcalc.EBIT[i]-inputcalc.DEPR[i] for i in range(len(inputcalc.SO))]
    Calculation ['EV/EBITDA'] = [Calculation.EV[i]/Calculation.EBITDA[i] for i in range(len(inputcalc.SO))]
    Calculation ['D/E'] = [inputcalc.TL[i]/inputcalc.TSE[i] for i in range(len(inputcalc.TL))]
    Calculation ['Debt/Totalcap'] = [(inputcalc.STD[i]+inputcalc.LTD[i])/(inputcalc.STD[i]+inputcalc.LTD[i]+inputcalc.TSE[i]) for i in range(len(inputcalc.STD))]
    Calculation ['Debt/EBITDA'] = [(inputcalc.STD[i]+inputcalc.LTD[i])/Calculation.EBITDA[i] for i in range(len(inputcalc.STD)) ]

    Calculation = Calculation.replace([np.inf, -np.inf], 0)
    Calculation = Calculation.fillna(0)
    #print(Calculation.tail(30))

    Calculation.to_csv(r'C:\Varians\Calculation.csv', index=None, header=True)
    return [Calculation, CFQ_reader, ISQ_reader, BSQ_reader, OV_reader]
