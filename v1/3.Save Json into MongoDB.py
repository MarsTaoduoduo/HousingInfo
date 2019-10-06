import json
from pymongo import MongoClient

db_name = '链家网爬取库'
city_xq_colname = '链家网城市及小区数据_201909'
city_colname = '链家网城市整体数据_201909'

with open('%s.json' % city_xq_colname,'r') as f:
    loadfile = json.load(f)

conn = MongoClient('127.0.0.1',27017)
db = conn[db_name]
collist = db.list_collection_names()
print(collist)
city_xq_mgdb = db[city_xq_colname]
if city_xq_colname in collist:city_xq_mgdb.drop()
city_xq_mgdb.insert_many(loadfile)

