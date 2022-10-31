from alpha_vantage.fundamentaldata import FundamentalData
from alpha_vantage.timeseries import TimeSeries
import numpy as np
import pandas as pd
import csv
import time

class ApiKeyDistribute() :
    def __init__(self) :
        API_KEY_TEST1 = 'G23MAAVKWB5TMPMV'
        API_KEY_TEST2 = '6PE06AMZAM1MCGFX' #myanuarfirdaus
        API_KEY_TEST3 = '6HZYCBP71FGOR1O5' #myanuarfirdaus23
        API_KEY_TEST4 = 'X5LNMI2AESR1YQCU' #muh_yanuar_firdaus@yahoo.com
        API_KEY_TEST5 = 'YBBKWRK4VSTP4GZH' #anggiengineer@yahoo.com
        API_KEY_TEST6 = 'X3Z3LQ31Z9B1OYX9' #myanuarfirdaus@hotmail.com
        self.APIlist = [API_KEY_TEST1,API_KEY_TEST2,API_KEY_TEST3,API_KEY_TEST4,API_KEY_TEST5,API_KEY_TEST6]
        
    def randomize(self, API_KEY_TEST) :
        self.API_KEY_TEST = API_KEY_TEST
        if (self.API_KEY_TEST == "" ) or (self.API_KEY_TEST == self.APIlist[(len(self.APIlist)-1)]) :
            self.API_KEY_TEST = self.APIlist[0]
            print ("API key is : " + self.API_KEY_TEST)
            return self.API_KEY_TEST
        else :
            self.API_KEY_TEST = self.APIlist[self.APIlist.index(self.API_KEY_TEST)+1]
            print ("API key is : " + self.API_KEY_TEST)
            return self.API_KEY_TEST
        return
    
class ApiCalling() :
    def __init__(self, entity, API_KEY_TEST) :
        self.entity = entity
        self.API_KEY_TEST = API_KEY_TEST
    def step_1(self ) :
        while True :
            try :
                self.fd = FundamentalData(key=self.API_KEY_TEST, output_format='json')
                data1, meta_data = self.fd.get_income_statement_quarterly(self.entity)  #get income statement quarterly data
                data1.to_csv(r'Varians\Income Statement Quarterly.csv', index=None, header=True)
                print("Succsesfully Pull Data Income Statement from web")
                break
            except ValueError as e :
                print (e)
                if str(e) == "Error getting data from the api, no return was given." :
                    print ("Problem Ticker Name : "+self.entity+" ...Skipping")
                    return 'Skip Ticker'
                    break
                    continue
                else :             
                    print ("Skip step_1")
                    time.sleep(20) 
                    self.API_KEY_TEST = ApiKeyDistribute().randomize(self.API_KEY_TEST)
    def step_2(self, e) :
        self.e = e
        while True :
            if self.e != 'Skip Ticker' :
                try :
                    self.fd = FundamentalData(key=self.API_KEY_TEST, output_format='json')
                    data2, meta_data = self.fd.get_cash_flow_quarterly(self.entity) #get cash flow quarterly data
                    data2.to_csv(r'Varians\Cash Flow Quarterly.csv', index=None, header=True)
                    print("Succsesfully Pull Data Cash Flow from web")
                    break
                except ValueError as e :
                    print (e)       
                    time.sleep(20) 
                    self.API_KEY_TEST = ApiKeyDistribute().randomize(self.API_KEY_TEST)
            else :
                print ("Skip step_2")
                break
                continue
    def step_3(self, e):
        self.e = e
        while True :
            if self.e != 'Skip Ticker' :
                try :
                    self.fd = FundamentalData(key=self.API_KEY_TEST, output_format='json')
                    data3, meta_data = self.fd.get_balance_sheet_quarterly(self.entity) #get balance sheet quarterly data
                    data3.to_csv(r'Varians\Balance Sheet Quarterly.csv', index=None, header=True)
                    print("Succsesfully Pull Data Balance Sheet from web")
                    break
                except ValueError as e :
                    print (e)                  
                    time.sleep(20) 
                    self.API_KEY_TEST = ApiKeyDistribute().randomize(self.API_KEY_TEST)
            else :
                print ("Skip step_3")
                break
                continue
    def step_4(self, e):
        self.e = e
        while True :
            if self.e != 'Skip Ticker' :
                try :
                    self.fd = FundamentalData(key=self.API_KEY_TEST, output_format='json')
                    step = "step_4"
                    OV, meta_data = self.fd.get_company_overview(self.entity)
                    with open(r'Varians\Overview.csv', 'w') as g:
                        writer1=csv.writer(g,lineterminator='\n')
                        writer1.writerow(OV.keys())
                        writer1.writerow(OV.values())
                    print("Succsesfully Pull Data Overview from web")
                    break
                except ValueError as e :
                    print (e)              
                    time.sleep(20) 
                    self.API_KEY_TEST = ApiKeyDistribute().randomize(self.API_KEY_TEST)
            else :
                print ("Skip step_4")
                break
                continue
    def step_5(self, e):
        self.e = e
        while True :
            if self.e != 'Skip Ticker' :
                try :
                    self.ts =  TimeSeries(key=self.API_KEY_TEST, output_format='pandas')
                    last_price, meta_data = self.ts.get_monthly_adjusted(self.entity)
                    last_price.to_csv(r'Varians\Last Price.csv', index=True, header=True)
                    print("Succsesfully Pull Data Timeseries from web")
                    break
                except ValueError as e :
                    print (e)        
                    time.sleep(20) 
                    self.API_KEY_TEST = ApiKeyDistribute().randomize(self.API_KEY_TEST)
                    if str(e) == "Invalid API call. Please retry or visit the documentation (https://www.alphavantage.co/documentation/) for TIME_SERIES_MONTHLY_ADJUSTED." :
                        self.e = 'Skip Ticker'
                        break
                        continue
                        
            else :
                print ("Skip step_5")
                break
                continue
        
if __name__ == "__main__":  
        API_KEY_TEST = ""
        entity = ["gagago", "AAPL"]
        for i in entity :
            API_KEY_TEST = ApiKeyDistribute().randomize(API_KEY_TEST)
            e = ApiCalling(i, API_KEY_TEST).step_1()
            ApiCalling(i, API_KEY_TEST).step_2(e)
            ApiCalling(i, API_KEY_TEST).step_3(e)
            ApiCalling(i, API_KEY_TEST).step_4(e)
            ApiCalling(i, API_KEY_TEST).step_5(e)
