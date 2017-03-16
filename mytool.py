# -*- coding: utf-8 -*-
'''
存放常量和经常使用模块
'''
import tushare as ts
import pandas as pd
import datetime
from sqlalchemy import create_engine



localdb = 'mysql://root:@127.0.0.1:3306/stock?charset=utf8'
serverdb = 'mysql://stock:bfstock330@rm-bp1l731gac61ue24q.mysql.rds.aliyuncs.com:3306/stock?charset=utf8'




#class存储tushare中的const变量
class ts_const:
    #指数code代码：get_hist_data接口指数代码
    code2ts = {'000001':'sh','399001':'sz','000300':'hs300','000016':'sz50','399005':'zxb','399006':'cyb'}


#day:检查目标日期
def ts_chkDaydata(day):
    i=1
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    #result = pd.DataFrame(columns=['stockId','data','open','close','high','low','volume','name','adjFactor'])
    #校验标志位
    result = 1
    #while i<=10:
    while i<= len(tab_stock):
        if tab_stock.type[i-1] == True:
            data_day = ts.get_h_data(tab_stock.code[i-1],start=day,end=day,index=True)
            data_day_hfq = ts.get_h_data(tab_stock.code[i-1],start=day,end=day,index=True,autype='hfq')
        else:
            data_day = ts.get_hist_data(tab_stock.code[i-1],start=day,end=day) 
            data_day_hfq = ts.get_h_data(tab_stock.code[i-1],start=day,end=day,autype='hfq')
            
        if type(data_day) == pd.core.frame.DataFrame and len(data_day)>0:
            data_day['name'] = tab_stock.name[i-1]
            data_day['stockId']=i
            data_day['adjFactor']=data_day_hfq['close']/data_day['close']
            #根据是否指数，补入当天PE PB的、
            data = data_day.reset_index()
            tab = data[['stockId','date','open','close','high','low','volume','name','adjFactor']]
            #校验操作
            data_day_sql = pd.read_sql(con=connect,sql='select * from hist_day where date="%s" and stockId=%d'%(day,i))
            #有数据说明已经存入进行校验，无数据则存入
            if len(data_day_sql)>0:
                pass
            else:
                tab.to_sql('hist_day',connect,if_exists='append',index=False)
                #加入2次校验，
                result = 0
        i = i+1
    return result
    
#day；程序启动日期
#bug：还要处理不开盘日期的情况
def check_whenopen(day):
    yesterday = getYesterday(day)
    #istarde = ts
    data_ok = ts_chkDaydata(yesterday)
    if data_ok:
        return
    else:
        return check_whenopen(yesterday)
           
         
         
         

def getYesterday(today):
    oneday = datetime.timedelta(days=1)
    today = datetime.datetime.strptime(today,'%Y-%m-%d')
    yesterday = (today-oneday).strftime('%Y-%m-%d')  
    return yesterday


