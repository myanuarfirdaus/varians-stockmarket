#function for making TTM data
import numpy as np
import pandas as pd
from pandas import DataFrame


def makettm(tabtemp,x):
    tabtemp = DataFrame(tabtemp)
    width = x
    shifted = tabtemp.shift(0)
    window = shifted.rolling(window=width)
    test = window.sum()
    complete = test.fillna(value=0)
    return complete

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

