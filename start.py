# -*- coding: utf-8 -*-
'''
9:30之前启动
'''

from data_ts import *
from data_tl import *
#from data_reduction import *
import logging
import logging.config
import threading

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('stock')  
logger.info('app start first')  



'''
if __name__ == '__main__':
    client = Client()
    client.init('ad67ea0d9557c129fca021250f34e198701e1dee6284893db5619bbce88a0fa4')
   
    year,month,today = datetime.datetime.now().year,datetime.datetime.now().month,datetime.datetime.now().day
    #日K数据暂定为晚上8点，确保tushare中复权数据已经产生
    sched_Timer = datetime.datetime(year,month,today,20,0,0)
    #tushare获取数据入口
    #startSave(sched_Timer)
    task_ts = threading.Thread(target=startSave,args=(sched_Timer,))
    #通联获取数据入口
    #start_work(client)
    task_tl = threading.Thread(target=start_work,args=(client,))
    
    task_ts.start()
    #task_tl.start()
    #每周的数据处理
    #start_data_reduction()
'''

if __name__ == '__main__':
    client = Client()
    client.init('ad67ea0d9557c129fca021250f34e198701e1dee6284893db5619bbce88a0fa4')
   
    year,month,today = datetime.datetime.now().year,datetime.datetime.now().month,datetime.datetime.now().day
    #日K数据暂定为晚上8点，确保tushare中复权数据已经产生
    sched_Timer = datetime.datetime(year,month,today,20,0,0)
    #通联获取数据入口
    #start_work(client)
    #tushare获取数据入口,记得放最后，这个函数包含一个while，放前面会造成函数堵塞
    startSave(sched_Timer)
    #每周的数据处理
    start_data_reduction()

    
    
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
