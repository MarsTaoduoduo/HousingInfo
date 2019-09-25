import json,operator
from pymongo import MongoClient

db_name = '链家网爬取库'
city_xq_colname = '链家网城市及小区数据_201909'
city_colname = '链家网城市整体数据_201909'

conn = MongoClient('127.0.0.1',27017)
db = conn[db_name]
collist = db.list_collection_names()
print(collist)
city_xq_mgdb = db[city_xq_colname]
city_xq = city_xq_mgdb.find()

city_price = []
for city in city_xq:
    price = 0
    num = 0
    for xiaoqu in city['小区列表']:
        if xiaoqu['二手房参考均价'] == '暂无':
            price_temp = 0
        else:
            price_temp = int(xiaoqu['二手房参考均价'])
        if xiaoqu['在售二手房套数'] == '暂无':
            num_temp = 0
        else:
            num_temp = int(xiaoqu['在售二手房套数'])
        price = price + price_temp * num_temp #用在售二手房套数作为加权因子计算城市平均房价
        num = num + num_temp

    if price > 0 and num > 0:
        print(city['省份'], '-', city['城市'],"均价：",round(price/num,0),'元')
        JsonText = {
            '省份': city['省份'],
            '城市': city['城市'],
            '均价': round(price/num,0),
            '在售二手房套数': num,
            '城市网址': city['城市网址']
        }
        city_price.append(JsonText)
print('排序前：\n',city_price)
city_price_sorted = sorted(city_price,key=operator.itemgetter('均价'),reverse=True) #根据均价对字典进行排序
print('排序后：\n',city_price_sorted)

with open('%s.json' % city_colname, 'w') as f:
    json.dump(city_price_sorted, f)  # pyth

# 需放在导出json之后，不然可能会报错
city_mgdb = db[city_colname]
if city_colname in collist:city_mgdb.drop()
city_mgdb.insert_many(city_price_sorted, ordered=True)