from sqlalchemy import create_engine
import tushare as ts
import pandas as pd

connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
print 'ok'
sql = 'select * from hist_day where stockId = 1'
df = pd.read_sql_query(sql,connect)
connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
df = pd.read_sql_query(sql,connect)
connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
df = pd.read_sql_query(sql,connect)
connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
df = pd.read_sql_query(sql,connect)
connect = create_engine('mysql://root:@127.0.0.1:3306/stock?charset=utf8')
df = pd.read_sql_query(sql,connect)
print 'conn'