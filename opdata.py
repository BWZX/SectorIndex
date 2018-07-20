# -*- coding: utf-8 -*-
import json
import pandas as pd
import tushare as ts
from datetime import datetime as dt
from datetime import timedelta as td
import calendar as cal
import numpy as np
import os
from tqdm import tqdm

# from opdata import factors as _factors
# from opdata.mongoconnect import *  
from mongoconnect import *

__T = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)),'calAll.csv'))
__TM = ts.get_k_data('000001', ktype='M', index=True)[['date']]

def get_day(code, start_date='2001-02-01', end_date='2017-10-10'):
    # if not start_date:
    #     start_date='2001-01-01'
    # if not end_date:
    #     end_date='2020-10-10'
    T= __T
    cursor = security.find({'code':code, 'date':{'$gte':start_date, '$lte': end_date}}).sort('date')
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return df  

    lastvalue = 0.0
    def setValue(v):
        nonlocal lastvalue
        if pd.isnull(v) or v == 'None':
            return lastvalue
        else:
            lastvalue = v
            return lastvalue
    T.rename(columns={'calendarDate':'date'}, inplace=True)
    T=T[T.date > '2001-01-01']
    T=T.merge(df,on='date',how='left', copy=False)
    T=T.drop_duplicates(['date'])
    T[['open']] = T[['open']].astype(float)
    T[['close']] = T[['close']].astype(float)
    T[['high']] = T[['high']].astype(float)
    T[['low']] = T[['low']].astype(float)
    T[['volume']] = T[['volume']].astype(float)
    lastvalue = 0.0
    T['high']=T['high'].apply(setValue)
    lastvalue = 0.0
    T['low']=T['low'].apply(setValue)
    lastvalue = 0.0
    T['close']=T['close'].apply(setValue)
    lastvalue = 0.0
    T['open']=T['open'].apply(setValue)
    lastvalue = 0.0
    T['volume']=T['volume'].apply(setValue)
    del T['_id']
    T = T[T.isOpen >0.5]
    T = T[T.date >= start_date]
    T = T[T.date <= end_date]
    del T['isOpen']
    T = T.reset_index()
    del T['index']
    del T['code'] 
    T['code'] = code
    return T        

def get_holdfund(code, start_date='2004-01-01', end_date='2017-12-12'):
    lastvalue = 0.0
    def setValue(v):
        nonlocal lastvalue
        if pd.isnull(v) or v == 'None' or v == '--':
            return lastvalue
        else:
            lastvalue = v
            return lastvalue
    cursor = holdfund.find({'code':str(code), 'date':{'$gte':'2004-01-01', '$lte': end_date}}).sort('date')
    hfdf = pd.DataFrame(list(cursor))
    if hfdf.empty:
        return hfdf
    df = pd.DataFrame(columns=['date','fund_holders','t_shares','t_share_rate','t_market_value','t_net_rate'])
    currentdate=''    
    fund_holders,t_shares,t_share_rate,t_market_value,t_net_rate = 0,0,0.0,0.0,0.0
    for col in range(len(hfdf)):
        if hfdf.iloc[col].date != currentdate:
            if fund_holders > 0:
                df.loc[len(df)] = [currentdate,fund_holders,t_shares,t_share_rate,t_market_value,t_net_rate]
            currentdate = hfdf.iloc[col].date
            fund_holders,t_shares,t_share_rate,t_market_value,t_net_rate = 0.0,0.0,0.0,0.0,0.0
        else:
            fund_holders +=1
            t_shares += float(hfdf.iloc[col].hold_shares)
            t_share_rate += float(hfdf.iloc[col].liquid_share_rate)
            t_market_value += float(hfdf.iloc[col].maket_value)
            t_net_rate += float(hfdf.iloc[col].net_value_rate)

    
    T = __T    
    T.rename(columns={'calendarDate':'date'}, inplace=True)
    # T = T[T.date > '2003-01-01']
    T=T.merge(df,on='date',how='left')
    T=T.drop_duplicates(['date'])
    for column in T:
        if column != 'code' and column != 'date':
            try:
                T[[column]] = T[[column]].astype(float)
            except ValueError:
                pass            
            lastvalue = 0.0
            T[column]=T[column].apply(setValue)
            
    T = T[T.isOpen >0.5]
    T = T[T.date >= start_date]
    T = T[T.date <= end_date]
    T['code']=str(code)
    T = T.reset_index()
    del T['index']
    del T['isOpen']
    return T


if __name__ == '__main__':
    # print(macrodata())
    # print(get_day('002236','2007-08-05','2010-08-05'))
    # _fetch_finance()
    # print(get_finance('000001'))
    # print(get_local_future('A99'))
    print(get_forecast('000001'))
    # print(get_holdfund('000001'))
    # print(get_future('XAU/USD'))
    # print(get_month('2010-01'))
    # print(get_ts_finance('000001','1m'))
    # re = get_all('test','1m','2010-01', ['open', 'vol_1_1m','rsi_10_1m', 'EBITDA2TA'],index=False)
    # print(re)
    # _fetch_forecast()
    # print(re[1])
    # print(re[2])    