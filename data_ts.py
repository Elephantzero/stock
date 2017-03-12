# -*- coding: utf-8 -*-
import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import datetime
import time
import threading
import urllib,urllib2,httplib

from mytool import *

import warnings
warnings.filterwarnings("ignore")


#tushare获取个股.指数的5min数据
def save_hist_5min(i):
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
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
    
data_recent = {} #dict   i:第i个指数上次获取的df
last_time = '' #存储上次获取具体5min数据的时间点
#tushare获取6个指数的5min数据    
def ts_save_index_5min(i):
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    
    start_day = datetime.date.today().strftime('%Y-%m-%d')
    
    
    print 'start save_index_5min  in     '+start_day+'...........................'
    
    global data_recent
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
                if i not in data_recent or data_5min.index[0] != data_recent[i].index[0]:
                    data_recent[i] = data_5min
                    #处理数据
                    data_5min['name'] = tab_stock.name[i-1]
                    data_5min['stockId']=i
                    data = data_5min.reset_index()
                    time = data.date[0]
                    global last_time
                    last_time = time
                    tab = data[['stockId','date','open','close','high','low','volume','name']]
                    tab.to_sql('hist_5min',connect,if_exists='append',index=False)
                    print 'get index_5min -->',i,'-->at',time
        i = i+1
        
            
    print 'succ save index_5min....................'+last_time

                    
                    
                    
                    
                    
def ts_circle_save_5min():
    timer = threading.Timer(300,ts_circle_save_5min)
    timer.start()
    ts_save_index_5min(1)    
    
    


        
#tushare获取个股.指数的day数据
def ts_save_hist_day(i):
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    
    start_day = datetime.date.today().strftime('%Y-%m-%d')
    
    
    check_whenopen(start_day)
    
    print 'start working in     '+start_day+'...........................'
        
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
                tab = data[['stockId','date','open','close','high','low','volume','name']]
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
                    tab = data[['stockId','date','open','close','high','low','volume','name','turnover','adjFactor','nmc','PB','PE']]
                    #增加数据库回滚
                    tab.to_sql('hist_day',connect,if_exists='append',index=False)
                except:
                    print '%s has no data_day'%i
            #股票—————————————————————————————————————————————————————
        i = i+1
        print 'get data_day -->',i
    print 'succ save data_day....................'+start_day
    
def ts_circle_save_day():
    timer = threading.Timer(86400,ts_circle_save_day)
    timer.start()
    ts_save_hist_day(1)      


def startSave(t1):
    flag_day = 1
    flag_5min = 1
    while True:
        if flag_5min:
            ts_circle_save_5min()
            flag_5min = 0
            
        now = datetime.datetime.now()
        if now >= t1 and flag_day:
           ts_circle_save_day()
           flag_day = 0
        
    
    
    
    
    
#心跳程序
def workstate():
    '''
    url = "http://localhost:5000/stockweb/heartbeat?state=ImWorking"
    req = urllib2.Request(url)
#    print req
#    res_data = urllib2.urlopen(req)
#    res = res_data.read()
#    print res
    '''
    while True:
        url = "http://localhost:5000/stockweb/heartbeat?state=ImWorking"
        conn = httplib.HTTPConnection("localhost:5000")
        conn.request(method="GET",url=url)
        time.sleep(300)

        
'''
if __name__ == '__main__':
    task_5min = threading.Thread(target=save_hist_5min,args=(1,))
    task_day = threading.Thread(target=save_hist_day,args=(1,))
   # work_state = threading.Thread(target=workstate)
    task_5min.start()
    task_day.start()
   # work_state.start()
    task_5min.join()
    task_day.join()
   # work_state.join()
''' 

if __name__ == '__main__':
    year,month,today = datetime.datetime.now().year,datetime.datetime.now().month,datetime.datetime.now().day
    sched_Timer = datetime.datetime(year,month,today,20,0,0)
    startSave(sched_Timer)


