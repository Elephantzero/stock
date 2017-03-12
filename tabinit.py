# -*- coding: utf-8 -*-
'''
更新tab
存储数据库连接等常量

'''
import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import re


def create_tabStock_industry():
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    tab_stock = pd.read_sql('stock',connect)
    tab_industry = pd.read_sql('industry',connect)
   
    
 #行业分类   
    df_industry = ts.get_industry_classified()
    #对表进行left join，使表增加stockid字段
    tab_1 = pd.merge(df_industry,tab_stock,how='left')
    tab_1.rename(columns={'Id': 'stockId'}, inplace=True)
    
    #对表进行left join where c_name==Name 增加industryid字段
    tab_2 = pd.merge(tab_1,tab_industry,how='left',left_on='c_name',right_on='Name') 
    tab_2.rename(columns={'Id': 'industryId'}, inplace=True)
    tab_3 = tab_2[['stockId','industryId']]
    #删除空数据
    tab_4 = tab_3.dropna(axis=0)
    tab_4.to_sql('stock_industry',connect,if_exists='append',index=False)
 #概念分类
    df_concept = ts.get_concept_classified() 
    tab_5 = pd.merge(df_concept,tab_stock,how='left')
    tab_5.rename(columns={'Id': 'stockId'}, inplace=True)
    tab_6 = pd.merge(tab_5,tab_industry,how='left',left_on='c_name',right_on='Name')
    tab_6.rename(columns={'Id': 'industryId'}, inplace=True)
    tab_7 = tab_6[['stockId','industryId']]
    tab_8 = tab_7.dropna(axis=0)
    tab_8.to_sql('stock_industry',connect,if_exists='append',index=False)

    
    
def exchange_cd(code_array):
    re_shang = re.compile(r'^6')
    re_shen = re.compile(r'^(3|0)')
    exchange_Cd = []
    for code in code_array:
        if re.match(re_shang,code):
            exchange_Cd.append(0)
        elif re.match(re_shen,code):
            exchange_Cd.append(1)
    return exchange_Cd
    
    
        
    
def create_tabStock():
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')

    #存储指数
    stockindex_info = ts.get_index()
    stockindexId = stockindex_info[['code','name']]
    stockindexId['exchangeCd'] = ''
    stockindexId['type'] = True
    stockindexId.to_sql('stock',connect,if_exists='append',index=False)
    
    #存储股票
    stock_info = ts.get_stock_basics()
    stockId = stock_info[['name']]
    stockId['exchangeCd'] = exchange_cd(stock_info.index)
    stockId['type'] = False
    s2 = stockId.reset_index()
    s2.to_sql('stock',connect,if_exists='append',index=False)

    


def create_tabIndustry():
    df_industry = ts.get_industry_classified()
    df_concept = ts.get_concept_classified()
    #合并2张表
    result=pd.concat([df_industry,df_concept])
    data = result.drop_duplicates(['c_name'])
    data_2 = pd.DataFrame({'Name':data.c_name })
    data_3 = data_2.reset_index(drop=True)
    connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
    data_3.to_sql('industry',connect,if_exists='append',index=False)
    
    
if __name__=='__main__':
    create_tabStock()
    create_tabIndustry()
    create_tabStock_industry()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    