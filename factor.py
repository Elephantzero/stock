# -*- coding: utf-8 -*-
import talib as ta
import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
'''
数据用前复权数据计算
前复权数据根据后复权因子自己计算
'''

'''
计算方法：
12日EMA的计算：EMA12 = 前一日EMA12 X 11/13 + 今日收盘 X 2/13
26日EMA的计算：EMA26 = 前一日EMA26 X 25/27 + 今日收盘 X 2/27
差离值（DIF）的计算： DIF = EMA12 - EMA26，即为talib-MACD返回值macd
根据差离值计算其9日的EMA，即离差平均值，是所求的DEA值。今日DEA = （前一日DEA X 8/10 + 今日DIF X 2/10），即为talib-MACD返回值signal
DIF与它自己的移动平均之间差距的大小一般BAR=（DIF-DEA)2，即为MACD柱状图。但是talib中MACD的计算是bar = (dif-dea)1
'''
def get_macd(df):
    macd, signal, hist = ta.MACD(df['qclose'].values, fastperiod=12, slowperiod=26, signalperiod=9)
    #return macd,signal,hist
    return macd

def get_boll(df):
    upper, middle, lower = ta.BBANDS(
        df['qclose'].values, 
        timeperiod=20,
        # number of non-biased standard deviations from the mean
        nbdevup=2,
        nbdevdn=2,
        # Moving average type: simple moving average here
        matype=0)
    return upper,middle,lower
    
def get_ma(df,tp):
    ma = ta.MA(df['qclose'].values, timeperiod=tp)
    return ma
'''
n日RSV=（Cn－Ln）/（Hn－Ln）×100
公式中，Cn为第n日收盘价；Ln为n日内的最低价；Hn为n日内的最高价。
其次，计算K值与D值：
当日K值=2/3×前一日K值+1/3×当日RSV
当日D值=2/3×前一日D值+1/3×当日K值
若无前一日K 值与D值，则可分别用50来代替。
J值=3*当日K值-2*当日D值
以9日为周期的KD线为例，即未成熟随机值，计算公式为
9日RSV=（C－L9）÷（H9－L9）×100
公式中，C为第9日的收盘价；L9为9日内的最低价；H9为9日内的最高价。
K值=2/3×第8日K值+1/3×第9日RSV
D值=2/3×第8日D值+1/3×第9日K值
J值=3*第9日K值-2*第9日D值
若无前一日K值与D值，则可以分别用50代替。
'''   
def get_kdj(df):
    k,d = ta.STOCH(high=df['qhigh'].values, 
                low=df['qlow'].values, 
                close=df['qclose'].values,
                fastk_period=9,
                slowk_period=3,
                slowk_matype=0,
                slowd_period=3,
                slowd_matype=0)
    return k,d
    
def get_qfq_data(stockid):
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    sql = 'select * from hist_day where stockId = %s'%stockid
    df = pd.read_sql_query(sql,connect)
    #将df按时间排序，日期最近的在最后,再计算
    df = df.sort(columns='date')
    df['qhigh'] = [df['high'][i]*df['adjFactor'][i]/df['adjFactor'].values[-1] for i in range(len(df))]
    df['qlow'] = [df['low'][i]*df['adjFactor'][i]/df['adjFactor'].values[-1] for i in range(len(df))]
    df['qclose'] = [df['close'][i]*df['adjFactor'][i]/df['adjFactor'].values[-1] for i in range(len(df))]
    df['qopen'] = [df['open'][i]*df['adjFactor'][i]/df['adjFactor'].values[-1] for i in range(len(df))]
    return df        

def get_all_factor(df):
    MACD = get_macd(df)
    BOLL = get_boll(df)
    MA5 = get_ma(df,5)
    MA10 = get_ma(df,10)
    MA20 = get_ma(df,20)
    MA30 = get_ma(df,30)
    MA60 = get_ma(df,60)
    KDJ = get_kdj(df)
    df['macd'] = MACD
    df['ma5'] = MA5
    df['ma10'] = MA10
    df['ma20'] = MA20
    df['ma30'] = MA30
    df['ma60'] = MA60
    result = df[['macd','ma5','ma10','ma20','ma30','ma60']]
    #result['histId'] = df[['Id']]
    result['histId'] = df['Id']
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    result.to_sql('factor',connect,if_exists='append',index=False)
 
    
def start_factor():
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    sql = 'select Id from stock'
    stock = pd.read_sql_query(sql,connect)
    for i in stock['Id']:
        data = get_qfq_data(i)
        get_all_factor(data)
        print 'calculate factor of stock%Id ok '%i





















    
if __name__ == "__main__":
    df = get_qfq_data(1)
    get_all_factor(df)
    
    
    
    
    
    
    
    
    
    
    
    
    