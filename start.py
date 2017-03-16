# -*- coding: utf-8 -*-
'''
9:30之前启动
'''

from data_ts import *
from data_tl import *
from data_reduction import *
import logging
import logging.config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('stock')  
logger.info('app start first')  




if __name__ == '__main__':
    client = Client()
    client.init('ad67ea0d9557c129fca021250f34e198701e1dee6284893db5619bbce88a0fa4')
   
    year,month,today = datetime.datetime.now().year,datetime.datetime.now().month,datetime.datetime.now().day
    #日K数据暂定为晚上8点，确保tushare中复权数据已经产生
    sched_Timer = datetime.datetime(year,month,today,20,0,0)
    #tushare获取数据入口
    startSave(sched_Timer)
    #通联获取数据入口
    start_work(client)
    #每周的数据处理
    start_data_reduction()
