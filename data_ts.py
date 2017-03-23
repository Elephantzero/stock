# -*- coding: utf-8 -*-
'''
主要调用接口说明：
get_hist_data:能获取各种ktype数据，未复权，区分指数和个股的方法是：000001为个股，sh为指数。只提供了6个指数
get_h_data:只有日K数据，提供各种复权数据，区分指数和个股的方法是：根据index字段
'''
import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime
import time
import threading
import urllib,urllib2,httplib
import calendar
import logging

from mytool import *
from factor import *

import warnings
warnings.filterwarnings("ignore")


logger = logging.getLogger('stock.ts')


#tushare获取个股.指数的5min数据
def save_hist_5min(i):
    #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    
    start_day = datetime.date.today().strftime('%Y-%m-%d')
    
    print 'start working in     '+start_day+'...........................'
    
    while i<= len(tab_stock):
   # while i<= 30:
    #应该增加time out异常处理
        try:                
            if tab_stock.type[i-1] == True:#指数
                if (tab_stock.code[i-1] in ts_const.code2ts.keys()):
                    data_5min = ts.get_hist_data(ts_const.code2ts[tab_stock.code[i-1]],ktype='5',start=start_day)
                else:
                    data_5min = pd.DataFrame()
            else:#非指数
                data_5min = ts.get_hist_data(tab_stock.code[i-1], ktype='5',start=start_day)
            
            #data_5min = ts.get_k_data(tab_stock.code[i-1], ktype='5',start=start_day,index=tab_stock.type[i-1])
        except:
            print 'get data_5min-->%s error'%i
            return save_hist_5min(i)
            #data_5min类型判断是否有数据
        if type(data_5min) == pd.core.frame.DataFrame and len(data_5min)>0:
            #取最近的5分钟线
            data_5min = data_5min.iloc[[0]]
            #处理数据
            data_5min['name'] = tab_stock.name[i-1]
            data_5min['stockId']=i
            data = data_5min.reset_index()
            tab = data[['stockId','date','open','close','high','low','volume','name']]
            tab.to_sql('hist_5min',connect,if_exists='append',index=False)
        i = i+1
        print 'get data_5min -->',i
        
    print 'succ save data_5min....................'+start_day

'''
tushare获取5min数据时，如果按照  start='2017-03-09 16:30' 可以获取5min的单条数据，但是超出开盘时间会造成time out，所以5min数据校验很麻烦。
校验方法：和日K一起校验昨日的5min是否正确，不正确则补充并校验前天数据
'''    
    
data_recent_5min = {} #dict   i:第i个指数上次获取的df
last_time_5min = '' #存储上次获取具体5min数据的时间点
#tushare获取6个指数的5min数据    
def ts_save_index_5min(i):
    #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    
    start_day = datetime.date.today().strftime('%Y-%m-%d')
    
    
    print 'start save_index_5min  in     '+start_day+'...........................'
    
    global data_recent_5min
    while i<= len(tab_stock):               
        if tab_stock.type[i-1] == True:#指数
            if (tab_stock.code[i-1] in ts_const.code2ts.keys()):
                data_5min = ts.get_hist_data(ts_const.code2ts[tab_stock.code[i-1]],ktype='5',start=start_day)
            else:
                data_5min = pd.DataFrame()

            #data_5min类型判断是否有数据
            if type(data_5min) == pd.core.frame.DataFrame and len(data_5min)>0:
                #取最近的5分钟线
                #将此数据与上次的数据进行比较，以此判断是否为开盘实时数据
                data_5min = data_5min.iloc[[0]]
                if i not in data_recent_5min or data_5min.index[0] != data_recent_5min[i].index[0]:
                    data_recent_5min[i] = data_5min
                    #处理数据
                    data_5min['name'] = tab_stock.name[i-1]
                    data_5min['stockId']=i
                    data = data_5min.reset_index()
                    this_time = data.date[0]
                    global last_time_5min
                    last_time_5min = this_time
                    tab = data[['stockId','date','open','close','high','low','volume','name']]
                    tab.to_sql('hist_5min',connect,if_exists='append',index=False)
                    print 'get index_5min -->',i,'-->at',this_time
        i = i+1
        
    logger.debug('succ save index_5min %s'%last_time_5min)        
    print 'succ save index_5min....................'+last_time_5min

                    
                    
                    
                    
                    
def ts_circle_save_5min():
    timer = threading.Timer(300,ts_circle_save_5min)
    timer.start()
    ts_save_index_5min(1)    
    
    


        
#tushare获取个股.指数的day数据
def ts_save_hist_day(i):
    #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    db_session=sessionmaker(bind=connect)
    session=db_session()
    tab_stock = pd.read_sql('stock',connect)
    
    start_day = datetime.date.today().strftime('%Y-%m-%d')
    
    
    check_whenopen(start_day)
    
    print 'start ts_save_hist_day  in     '+start_day+'...........................'
        
    otherdata_day = ts.get_today_all()
    while i<= len(tab_stock):
    #while i<= 30:
        if tab_stock.type[i-1] == True: 
            #指数——————————————————————————————————————————————————
            try:
                data_day = ts.get_h_data(tab_stock.code[i-1],start=start_day,index=True)
            except:
                print 'get data_day-->%s error'%i
                return ts_save_hist_day(i)
            if type(data_day) == pd.core.frame.DataFrame and len(data_day)>0:
                data_day['name'] = tab_stock.name[i-1]
                data_day['stockId']=i
                data = data_day.reset_index()
                date = data['date'][0]
                tab = data[['stockId','date','open','close','high','low','volume','name']]
                tab['adjFactor'] = 1
                sql = "update hist_5min,hist_15min,hist_30min,hist_60min set hist_5min.adjFactor=1,hist_15min.adjFactor=1,hist_30min.adjFactor=1,hist_60min.adjFactor=1 where hist_5min.stockId=%d and hist_15min.stockId=%d and hist_30min.stockId=%d and hist_60min.stockId=%d and hist_5min.date>'%s' and hist_15min.date>'%s' and hist_30min.date>'%s' and hist_60min.date>'%s'"%(i,i,i,i,date,date,date,date)
                session.execute(sql)
                session.commit()
                
                tab.to_sql('hist_day',connect,if_exists='append',index=False)
            #指数——————————————————————————————————————————————————
        else:
            #股票——————————————————————————————————————————————————
            try:
                
                data_day = ts.get_hist_data(tab_stock.code[i-1],start=start_day)
                data_day_hfq = ts.get_h_data(tab_stock.code[i-1],start=start_day,autype='hfq')
            except:
                print 'get data_day-->%s error'%i
                return ts_save_hist_day(i)
            #data_day类型判断是否有数据
            if type(data_day) == pd.core.frame.DataFrame and len(data_day)>0:
                data_day['name'] = tab_stock.name[i-1]
                data_day['stockId']=i
                try:
                    data_day['nmc']=otherdata_day.query('code=="%s"'%tab_stock.code[i-1]).nmc.values[0]
                    data_day['PB']=otherdata_day.query('code=="%s"'%tab_stock.code[i-1]).pb.values[0]
                    data_day['PE']=otherdata_day.query('code=="%s"'%tab_stock.code[i-1]).per.values[0]
                    data_day['adjFactor']=data_day_hfq['close']/data_day['close']
                    data = data_day.reset_index()
                    date = data['date'][0]
                    tab = data[['stockId','date','open','close','high','low','volume','name','turnover','adjFactor','nmc','PB','PE']]
                    sql = "update hist_5min,hist_15min,hist_30min,hist_60min set hist_5min.adjFactor=%f,hist_15min.adjFactor=%f,hist_30min.adjFactor=%f,hist_60min.adjFactor=%f where hist_5min.stockId=%d and hist_15min.stockId=%d and hist_30min.stockId=%d and hist_60min.stockId=%d and hist_5min.date>'%s' and hist_15min.date>'%s' and hist_30min.date>'%s' and hist_60min.date>'%s'"%(data_day['adjFactor'][0],data_day['adjFactor'][0],data_day['adjFactor'][0],data_day['adjFactor'][0],i,i,i,i,date,date,date,date)
                    session.execute(sql)
                    session.commit()
                    
                    #增加数据库回滚
                    tab.to_sql('hist_day',connect,if_exists='append',index=False)
                except:
                    print '%s has no data_day'%i
            #股票—————————————————————————————————————————————————————
        i = i+1
        print 'get data_day -->',i
        
        
    logger.debug('succ save data_day %s'%start_day)            
    print 'succ save data_day....................'+start_day
    #开始计算指标
    print 'start calculate factor................'+start_day
    start_factor()
    logger.debug('succ save factor %s'%start_day)
    print 'succ calculate factor................'+start_day
    

data_recent_15min = {} #dict   i:第i个指数上次获取的df
last_time_15min = '' #存储上次获取具体15min数据的时间点
#tushare获取6个指数的15min数据    
def ts_save_index_15min(i):
    #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    
    start_day = datetime.date.today().strftime('%Y-%m-%d')
    
    
    print 'start save_index_15min  in     '+start_day+'...........................'
    
    global data_recent_15min
    while i<= len(tab_stock):               
        if tab_stock.type[i-1] == True:#指数
            if (tab_stock.code[i-1] in ts_const.code2ts.keys()):
                data_15min = ts.get_hist_data(ts_const.code2ts[tab_stock.code[i-1]],ktype='15',start=start_day)
            else:
                data_15min = pd.DataFrame()

            #data_15min类型判断是否有数据
            if type(data_15min) == pd.core.frame.DataFrame and len(data_15min)>0:
                #取最近的15分钟线
                #将此数据与上次的数据进行比较，以此判断是否为开盘实时数据
                data_15min = data_15min.iloc[[0]]
                if i not in data_recent_15min or data_15min.index[0] != data_recent_15min[i].index[0]:
                    data_recent_15min[i] = data_15min
                    #处理数据
                    data_15min['name'] = tab_stock.name[i-1]
                    data_15min['stockId']=i
                    data = data_15min.reset_index()
                    this_time = data.date[0]
                    global last_time_15min
                    last_time_15min = this_time
                    tab = data[['stockId','date','open','close','high','low','volume','name']]
                    tab.to_sql('hist_15min',connect,if_exists='append',index=False)
                    print 'get index_15min -->',i,'-->at',this_time
        i = i+1
        
            
    print 'succ save index_15min....................'+last_time_15min



data_recent_30min = {} #dict   i:第i个指数上次获取的df
last_time_30min = '' #存储上次获取具体5min数据的时间点
def ts_save_index_30min(i):
    #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    
    start_day = datetime.date.today().strftime('%Y-%m-%d')
    
    
    print 'start save_index_30min  in     '+start_day+'...........................'
    
    global data_recent_30min
    while i<= len(tab_stock):               
        if tab_stock.type[i-1] == True:#指数
            if (tab_stock.code[i-1] in ts_const.code2ts.keys()):
                data_30min = ts.get_hist_data(ts_const.code2ts[tab_stock.code[i-1]],ktype='30',start=start_day)
            else:
                data_30min = pd.DataFrame()

            #data_30min类型判断是否有数据
            if type(data_30min) == pd.core.frame.DataFrame and len(data_30min)>0:
                #取最近的15分钟线
                #将此数据与上次的数据进行比较，以此判断是否为开盘实时数据
                data_30min = data_30min.iloc[[0]]
                if i not in data_recent_30min or data_30min.index[0] != data_recent_15min[i].index[0]:
                    data_recent_30min[i] = data_30min
                    #处理数据
                    data_30min['name'] = tab_stock.name[i-1]
                    data_30min['stockId']=i
                    data = data_30min.reset_index()
                    this_time = data.date[0]
                    global last_time_30min
                    last_time_30min = this_time
                    tab = data[['stockId','date','open','close','high','low','volume','name']]
                    tab.to_sql('hist_30min',connect,if_exists='append',index=False)
                    print 'get index_30min -->',i,'-->at',this_time
        i = i+1
        
            
    print 'succ save index_30min....................'+last_time_30min
    
    
    
data_recent_60min = {} #dict   i:第i个指数上次获取的df
last_time_60min = '' #存储上次获取具体5min数据的时间点    
def ts_save_index_60min(i):    
    #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    
    start_day = datetime.date.today().strftime('%Y-%m-%d')
    
    
    print 'start save_index_60min  in     '+start_day+'...........................'
    
    global data_recent_60min
    while i<= len(tab_stock):               
        if tab_stock.type[i-1] == True:#指数
            if (tab_stock.code[i-1] in ts_const.code2ts.keys()):
                data_60min = ts.get_hist_data(ts_const.code2ts[tab_stock.code[i-1]],ktype='60',start=start_day)
            else:
                data_60min = pd.DataFrame()

            #data_15min类型判断是否有数据
            if type(data_60min) == pd.core.frame.DataFrame and len(data_60min)>0:
                #取最近的15分钟线
                #将此数据与上次的数据进行比较，以此判断是否为开盘实时数据
                data_60min = data_60min.iloc[[0]]
                if i not in data_recent_60min or data_60min.index[0] != data_recent_60min[i].index[0]:
                    data_recent_60min[i] = data_60min
                    #处理数据
                    data_60min['name'] = tab_stock.name[i-1]
                    data_60min['stockId']=i
                    data = data_60min.reset_index()
                    this_time = data.date[0]
                    global last_time_60min
                    last_time_60min = this_time
                    tab = data[['stockId','date','open','close','high','low','volume','name']]
                    tab.to_sql('hist_60min',connect,if_exists='append',index=False)
                    print 'get index_60min -->',i,'-->at',this_time
        i = i+1
        
            
    print 'succ save index_15min....................'+last_time_60min
    
    
    
def ts_circle_save_15min():
    timer = threading.Timer(900,ts_circle_save_15min)
    timer.start()
    ts_save_index_15min(1)
    
    
def ts_circle_save_30min():
    timer = threading.Timer(1800,ts_circle_save_5min)
    timer.start()
    ts_save_index_30min(1)
    
    
def ts_circle_save_60min():
    timer = threading.Timer(3600,ts_circle_save_60min)
    timer.start()
    ts_save_index_60min(1)

#计算tp时间内的bar数据
def ts_my_data(stockid,tp):
    day = datetime.datetime.today()
    day_end = day.strftime("%Y-%m-%d")
    if tp == 'week':
        tp_day = day-datetime.timedelta(4)
        day_begin = tp_day.strftime("%Y-%m-%d")
    elif tp == 'month':
        tp_day = time.localtime()  
        day_begin = '%d-%02d-01' % (tp_day.tm_year, tp_day.tm_mon)  # 月初肯定是1号
    
    #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    sql = "select * from hist_day where stockId = %s and date>='%s' and date<='%s'"%(stockid,day_begin,day_end)
    df = pd.read_sql_query(sql,connect)
    #将df按时间排序，日期最近的在最后,再计算
    df = df.sort(columns='date')
    df['qhigh'] = [df['high'][i]*df['adjFactor'][i]/df['adjFactor'].values[-1] for i in range(len(df))]
    df['qlow'] = [df['low'][i]*df['adjFactor'][i]/df['adjFactor'].values[-1] for i in range(len(df))]
    df['qclose'] = [df['close'][i]*df['adjFactor'][i]/df['adjFactor'].values[-1] for i in range(len(df))]
    df['qopen'] = [df['open'][i]*df['adjFactor'][i]/df['adjFactor'].values[-1] for i in range(len(df))]
    tp_open = df.iloc[[0]]['qopen']
    tp_close = df.iloc[[-1]]['qclose']
    tp_high = max(df['qhigh'])
    tp_low = min(df['qlow'])
    tp_df = pd.DataFrame({'stockId':stockid,'date':day_end,'open':tp_open,'close':tp_close,'high':tp_high,'low':tp_low})
    return tp_df
    
#周五更新
def ts_save_hist_week(i):
    bStart = is_friday()
    if bStart:
        #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
        tab_stock = pd.read_sql('stock',connect)
        for i in tab_stock['Id'].values:
           df = ts_my_data(i,'week')
           df.to_sql('hist_week',connect)
            


#月底更新
def ts_save_hist_month(i):
    bStart = is_monthday()
    if bStart:
        #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
        tab_stock = pd.read_sql('stock',connect)
        for i in tab_stock['Id'].values:
           df = ts_my_data(i,'month')
           df.to_sql('hist_month',connect)
            
        




def is_friday():
    dayOfWeek = datetime.datetime.today().weekday()
    if dayOfWeek == 4:
        return True
    else:
        return False

def is_monthday():
    year = datetime.datetime.today().year
    month = datetime.datetime.today().month
    day = datetime.datetime.today().day
    if day==calendar.monthrange(year,month)[1]:
        return True
    else:
        return False

    
def ts_circle_save_day():
    timer = threading.Timer(86400,ts_circle_save_day)
    timer.start()
    ts_save_hist_day(1)

def ts_circle_save_week():
    timer = threading.Timer(86400,ts_circle_save_week)
    timer.start()    
    w_update = is_friday()
    if w_update:
        ts_save_hist_week(1)
        
        
def ts_circle_save_month():
    timer = threading.Timer(86400,ts_circle_save_month)
    timer.start()    
    w_update = is_monthday()
    if w_update:
        ts_save_hist_month(1)
        
      


def startSave(t1):
    flag_day = 1

    ts_circle_save_5min()
    ts_circle_save_15min()
    ts_circle_save_30min()
    ts_circle_save_60min()
             
    while flag_day:        
        now = datetime.datetime.now()
        if now >= t1:
           ts_circle_save_day()
           ts_circle_save_week()
           ts_circle_save_month()
           flag_day = 0
        time.sleep(10)

    
    

if __name__ == '__main__':
    year,month,today = datetime.datetime.now().year,datetime.datetime.now().month,datetime.datetime.now().day
    sched_Timer = datetime.datetime(year,month,today,20,0,0)
    startSave(sched_Timer)


