# -*- coding: utf-8 -*-
from dataapiclient import Client
import json
import MySQLdb
import datetime
import threading
import logging
import time



logger = logging.getLogger('stock.tl')
#localdb = 'mysql://root:@127.0.0.1:3306/stock?charset=utf8'
#serverdb = 'mysql://stock:bfstock330@rm-bp1l731gac61ue24q.mysql.rds.aliyuncs.com:3306/stock?charset=utf8'

#db = MySQLdb.connect(host="rm-bp1l731gac61ue24q.mysql.rds.aliyuncs.com",user="stock",passwd="bfstock330",db="stock",charset="utf8")
#cursor = db.cursor()

#db = MySQLdb.connect(host="127.0.0.1",user="stock",passwd="bfstock330",db="stock",charset="utf8") 

'''
通联数据的分钟线校验需要历史数据接口才能完善
'''


def bool_exchangeCd(cd):
    if cd == 'XSHG':
        return 0
    else:
        return 1
'''   
def tl_get_hist_1min(client):    
    today = datetime.date.today().strftime('%Y-%m-%d')
    start_time = datetime.datetime.now().strftime('%H:%M')
    try:
        url1='/api/market/getBarRTIntraDayOneMinute.json?time=%s&exchangeCD='%start_time
        code, result = client.getData(url1)
        if code==200:
            print 'get data_hist_1min from tl'
        else:
            print code
            print result
    except Exception, e:
        #traceback.print_exc()
        raise e
        
    r = json.loads(result)
    if r['retCode'] == 1:    
        #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
        #db_session=sessionmaker(bind=connect)
        #session=db_session()
        i = 1
        for data in r['data']:
            sql_stockid = 'select * from stock where code="%s" and exchangeCd=%d'%(data['ticker'],bool_exchangeCd(data['exchangeCD']))
            cursor.execute(sql_stockid)
            sql_stockid_result = cursor.fetchall()
            if len(sql_stockid_result)>0:
                stockid = sql_stockid_result[0][0]
                date = today+' '+data['barTime']
                op = data['openPrice']
                clo = data['closePrice']
                high = data['highPrice']
                low = data['lowPrice']
                name = data['shortNM']
                volume = data['totalVolume']
                print 'start save ...............'
                sql = 'insert into hist_1min(stockid,date,open,close,high,low,name,volume) values(%d,"%s",%f,%f,%f,%f,"%s",%f)'%(stockid,date,op,clo,high,low,name,volume)
                print sql
                cursor.execute(sql)
                db.commit()
            
            print i 
            i = i+1
    elif r['retCode'] == -1:
        print "on the exchange,can't get get_hist_1min................... "
'''
        
        
def tl_get_hist_5min(client):
    today = datetime.date.today().strftime('%Y-%m-%d')
    now_hour,now_min = datetime.datetime.now().hour,datetime.datetime.now().minute
    str_min = str(now_min/5*5)
    if str_min == '0':
        str_min = '00'
    elif str_min == '5':
        str_min = '05'
    
    start_time = str(now_hour)+':'+str_min
    
    try:
        url1='/api/market/getBarRTIntraDayOneMinute.json?time=%s&exchangeCD=&unit=5'%start_time
        #url1='/api/market/getBarRTIntraDayOneMinute.json?time=15:00&exchangeCD=&unit=5'
        code, result = client.getData(url1)
        if code==200:
            print 'get data_hist_5min from tl'
        else:
            print code
            print result
    except Exception, e:
        #traceback.print_exc()
        raise e
    r = json.loads(result)
    if r['retCode'] == 1:    
        #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
        #db_session=sessionmaker(bind=connect)
        #session=db_session()
        db = MySQLdb.connect(host='127.0.0.1',user='root',passwd="",db="stock",charset="utf8") 
        cursor = db.cursor()
        i = 1
        for data in r['data']:
            sql_stockid = 'select * from stock where code="%s" and exchangeCd=%d'%(data['ticker'],bool_exchangeCd(data['exchangeCD']))
            cursor.execute(sql_stockid)
            sql_stockid_result = cursor.fetchall()
            if len(sql_stockid_result)>0:
                stockid = sql_stockid_result[0][0]
                date = today+' '+data['barTime']
                op = data['openPrice']
                clo = data['closePrice']
                high = data['highPrice']
                low = data['lowPrice']
                name = data['shortNM']
                volume = data['totalVolume']
                print 'start save ...............'
                sql = 'insert into hist_5min(stockid,date,open,close,high,low,name,volume) values(%d,"%s",%f,%f,%f,%f,"%s",%f)'%(stockid,date,op,clo,high,low,name,volume)
                print sql
                cursor.execute(sql)
                db.commit()
                #session.close()
            
            print i 
            i = i+1
        db.close()
    elif r['retCode'] == -1:
        print "on the exchange,can't get get_hist_5min................... "

        
        
def tl_get_hist_15min(client):
    today = datetime.date.today().strftime('%Y-%m-%d')
    now_hour,now_min = datetime.datetime.now().hour,datetime.datetime.now().minute
    str_min = str(now_min/15*15)
    if str_min == '0':
        str_min = '00'

    start_time = str(now_hour)+':'+str_min
    
    try:
        #url1='/api/market/getBarRTIntraDayOneMinute.json?time=15:00&exchangeCD=&unit=15'
        url1='/api/market/getBarRTIntraDayOneMinute.json?time=%s&exchangeCD=&unit=15'%start_time
        code, result = client.getData(url1)
        if code==200:
            print 'get data_hist_15min from tl'
        else:
            print code
            print result
    except Exception, e:
        #traceback.print_exc()
        raise e
    r = json.loads(result)
    if r['retCode'] == 1:    
        #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
        #db_session=sessionmaker(bind=connect)
        #session=db_session()
        db = MySQLdb.connect(host="127.0.0.1",user="root",passwd="",db="stock",charset="utf8") 
        cursor = db.cursor()        
        i = 1
        for data in r['data']:
            sql_stockid = 'select * from stock where code="%s" and exchangeCd=%d'%(data['ticker'],bool_exchangeCd(data['exchangeCD']))
            cursor.execute(sql_stockid)
            sql_stockid_result = cursor.fetchall()
            if len(sql_stockid_result)>0:
                stockid = sql_stockid_result[0][0]
                date = today+' '+data['barTime']
                op = data['openPrice']
                clo = data['closePrice']
                high = data['highPrice']
                low = data['lowPrice']
                name = data['shortNM']
                volume = data['totalVolume']
                print 'start save ...............'
                sql = 'insert into hist_15min(stockid,date,open,close,high,low,name,volume) values(%d,"%s",%f,%f,%f,%f,"%s",%f)'%(stockid,date,op,clo,high,low,name,volume)
                print sql
                cursor.execute(sql)
                db.commit()
            
            print i 
            i = i+1
        db.close()
    elif r['retCode'] == -1:
        print "on the exchange,can't get get_hist_15min................... "

def tl_get_hist_30min(client):
    today = datetime.date.today().strftime('%Y-%m-%d')
    now_hour,now_min = datetime.datetime.now().hour,datetime.datetime.now().minute
    str_min = str(now_min/30*30)
    if str_min == '0':
        str_min = '00'
    start_time = str(now_hour)+':'+str_min
    try:
        url1='/api/market/getBarRTIntraDayOneMinute.json?time=%s&exchangeCD=&unit=30'%start_time
        code, result = client.getData(url1)
        if code==200:
            print 'get data_hist_30min from tl'
        else:
            print code
            print result
    except Exception, e:
        #traceback.print_exc()
        raise e
    r = json.loads(result)
    if r['retCode'] == 1:    
        #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
        #db_session=sessionmaker(bind=connect)
        #session=db_session()
        db = MySQLdb.connect(host="127.0.0.1",user="root",passwd="",db="stock",charset="utf8") 
        cursor = db.cursor()
        i = 1
        for data in r['data']:
            sql_stockid = 'select * from stock where code="%s" and exchangeCd=%d'%(data['ticker'],bool_exchangeCd(data['exchangeCD']))
            cursor.execute(sql_stockid)
            sql_stockid_result = cursor.fetchall()
            if len(sql_stockid_result)>0:
                stockid = sql_stockid_result[0][0]
                date = today+' '+data['barTime']
                op = data['openPrice']
                clo = data['closePrice']
                high = data['highPrice']
                low = data['lowPrice']
                name = data['shortNM']
                volume = data['totalVolume']
                print 'start save ...............'
                sql = 'insert into hist_30min(stockid,date,open,close,high,low,name,volume) values(%d,"%s",%f,%f,%f,%f,"%s",%f)'%(stockid,date,op,clo,high,low,name,volume)
                print sql
                cursor.execute(sql)
                db.commit()
            print i 
            i = i+1
        db.close()
    elif r['retCode'] == -1:
        print "on the exchange,can't get get_hist_30min................... "

def tl_get_hist_60min(client):
    today = datetime.date.today().strftime('%Y-%m-%d')
    now_hour,now_min = datetime.datetime.now().hour,datetime.datetime.now().minute
    #str_min = str(now_min/60*60)
    #start_time = str(now_hour)+':'+str_min
    if now_min/30:
        str_hour = str(now_hour)
    else:
        str_hour = str(now_hour-1)
    start_time = str_hour+':'+'30'
     
    try:
        url1='/api/market/getBarRTIntraDayOneMinute.json?time=%s&exchangeCD=&unit=60'%start_time
        code, result = client.getData(url1)
        if code==200:
            print 'get data_hist_60min from tl'
        else:
            print code
            print result
    except Exception, e:
        #traceback.print_exc()
        raise e
    r = json.loads(result)
    if r['retCode'] == 1:    
        #connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
        #db_session=sessionmaker(bind=connect)
        #session=db_session()
        db = MySQLdb.connect(host="127.0.0.1",user="root",passwd="",db="stock",charset="utf8") 
        cursor = db.cursor()
        i = 1
        for data in r['data']:
            sql_stockid = 'select * from stock where code="%s" and exchangeCd=%d'%(data['ticker'],bool_exchangeCd(data['exchangeCD']))
            cursor.execute(sql_stockid)
            sql_stockid_result = cursor.fetchall()
            if len(sql_stockid_result)>0:
                stockid = sql_stockid_result[0][0]
                date = today+' '+data['barTime']
                op = data['openPrice']
                clo = data['closePrice']
                high = data['highPrice']
                low = data['lowPrice']
                name = data['shortNM']
                volume = data['totalVolume']
                print 'start save ...............'
                sql = 'insert into hist_60min(stockid,date,open,close,high,low,name,volume) values(%d,"%s",%f,%f,%f,%f,"%s",%f)'%(stockid,date,op,clo,high,low,name,volume)
                print sql
                cursor.execute(sql)
                db.commit()
            print i 
            i = i+1
        db.close()
    elif r['retCode'] == -1:
        print "not on the exchange,can't get get_hist_60min................... "

        
        
        
        
        
'''        
def fun_timer(b_start):
    b_start = True
    timer = threading.Timer(60,fun_timer,[b_start])
    timer.start()
    print b_start
    if b_start:
        get_hist_1min(client)
        b_start = False
'''
    

'''
def tl_circle_save_1min(client):
    timer = threading.Timer(60,tl_circle_save_1min,[client])
    timer.start()
    tl_get_hist_1min(client)  
'''
'''
#标志位定时反转
def timer_flag_5min():
    global flag_5min
    flag_5min = 1
    return flag_5min
    
flag_5min = 1
def tl_circle_save_5min(client):
    global flag_5min
    while True:
        if flag_5min:
            task_time = threading.Timer(300,timer_flag_5min)
            task_time.start()
            flag_5min = 0
            tl_get_hist_5min(client)

def timer_flag_15min():
    global flag_15min
    flag_15min = 1
    return flag_15min
    
flag_15min = 1
def tl_circle_save_15min(client):
    global flag_15min
    while True:
        if flag_15min:
            task_time = threading.Timer(900,timer_flag_15min)
            task_time.start()
            flag_15min = 0
            tl_get_hist_15min(client)
            
def timer_flag_30min():
    global flag_30min
    flag_30min = 1
    return flag_30min
    
flag_30min = 1
def tl_circle_save_30min(client):
    global flag_30min
    while True:
        if flag_30min:
            task_time = threading.Timer(300,timer_flag_30min)
            task_time.start()
            flag_30min = 0
            tl_get_hist_30min(client)
        
        
def timer_flag_60min():
    global flag_60min
    flag_60min = 1
    return flag_60min
    
flag_60min = 1
def tl_circle_save_60min(client):
    global flag_60min
    while True:
        if flag_60min:
            task_time = threading.Timer(300,timer_flag_60min)
            task_time.start()
            flag_60min = 0
            tl_get_hist_60min(client)
            
def start_work(client):
    t1 = threading.Thread(target=tl_circle_save_5min,args=(client,))
    t2 = threading.Thread(target=tl_circle_save_15min,args=(client,))
    t3 = threading.Thread(target=tl_circle_save_30min,args=(client,))
    t4 = threading.Thread(target=tl_circle_save_60min,args=(client,))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
'''    
    
    
def tl_circle_save_5min(client):
    timer = threading.Timer(300,tl_circle_save_5min,[client])
    timer.start()
    tl_get_hist_5min(client)       

def tl_circle_save_15min(client):
    timer = threading.Timer(900,tl_circle_save_15min,[client])
    timer.start()
    tl_get_hist_15min(client)

def tl_circle_save_30min(client):
    timer = threading.Timer(1800,tl_circle_save_30min,[client])
    timer.start()
    tl_get_hist_30min(client) 

def tl_circle_save_60min(client):
    timer = threading.Timer(3600,tl_circle_save_60min,[client])
    timer.start()
    tl_get_hist_60min(client)         
        
def start_work(client):
    #tl_circle_save_1min(client)
    tl_circle_save_5min(client)
    tl_circle_save_15min(client)
    tl_circle_save_30min(client)
    tl_circle_save_60min(client)


    

if __name__ == "__main__":
    client = Client()
    client.init('ad67ea0d9557c129fca021250f34e198701e1dee6284893db5619bbce88a0fa4')
    start_work(client)

            
    
    
        
