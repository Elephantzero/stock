# -*- coding: utf-8 -*-
from datetime import datetime,date
from mytool import *
import pandas as pd
import threading
from sqlalchemy.orm import sessionmaker


db_session=sessionmaker(bind=connect)
session=db_session()


def is_Saturday():
    dayOfWeek = date.today().weekday()
    #print dayOfWeek
    if dayOfWeek == 5:
        return True
    else:
        return False

#创建新表：每年新建一张存放历史5分钟数据的表
def newYaerTable():
    thisYaer = datetime.now().year
    tab_name = 'hist_5min_%s'%thisYaer
    sql = 'SELECT table_name FROM information_schema.TABLES WHERE table_name="%s"'%tab_name
    tablist = session.execute(sql).fetchall()
    if tablist:
        pass
    else:
        sql = 'CREATE TABLE %s(Id int(11) not null primary key auto_increment,stockId int(11),date datetime,open float(10,2),high float(10,2),low float(10,2),close float(10,2),volume float(10,2),name varchar(10))'%tab_name
        session.execute(sql)
        session.commit()
        

def reduction_5min():
    thisYaer = datetime.now().year
    tab_name = 'hist_5min_%s'%thisYaer
    sql = 'SELECT table_name FROM information_schema.TABLES WHERE table_name="%s"'%tab_name
    tablist = session.execute(sql).fetchall()
    if tablist:
        pass
    else:
        sql = 'CREATE TABLE %s(Id int(11) not null primary key auto_increment,stockId int(11),date datetime,open float(10,2),high float(10,2),low float(10,2),close float(10,2),volume float(10,2),name varchar(10))'%tab_name
        session.execute(sql)
        session.commit()
    hist_5min = pd.read_sql_query('select stockId,date,open,close,high,low,volume,name from hist_5min',connect)
    hist_5min.to_sql(tab_name,connect,if_exists='append',index=False)
    sql = 'delete from hist_5min'
    #db_session=sessionmaker(bind=connect)
    #session=db_session()
    session.execute(sql)
    session.commit()
'''
def alldata_reduction():
    bStart = is_Saturday()
    if bStart:
        reduction_5min()
        print 'alldata_reduction is  working'
    print 'alldata_reduction end.............'
'''    
def start_data_reduction():
    timer = threading.Timer(86400,reduction_5min)
    timer.start()
    print 'start start_data_reduction'
    reduction_5min()
    
        
