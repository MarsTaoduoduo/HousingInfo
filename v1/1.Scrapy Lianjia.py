import requests
from bs4 import BeautifulSoup
import time,json,datetime
from pymongo import MongoClient



# 通用：请求网页
#----------------------------------------------------------------------------------------
def request_page(url,headers):
    try:
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except requests.exceptions.RequestException:
        print('请求详情页出错',url)
        return None

# 获取所有城市及省份的url列表
#----------------------------------------------------------------------------------------
def city_url_list():
    city_url = 'https://www.lianjia.com/city/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
        'Referer': 'https://sz.lianjia.com/'
    }
    html = request_page(city_url,headers)
    cities = parse_city_detail(html)
    return cities


def parse_city_detail(html):
    soup = BeautifulSoup(html,'lxml')
    cities = []
    for province in soup.find_all(attrs={'class': 'city_province'}):
        for city in province.find_all(name='a'):
           JsonText = {
               '省份': province.div.get_text(),
               '城市': city.get_text(),
               '城市网址': city.attrs['href']
           }
           cities.append(JsonText)
    print(cities)
    return cities

# 获取单个城市全部小区信息
#----------------------------------------------------------------------------------------

def xiaoqu_pn_list(url): #在pn页的小区信息
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
        'Referer': city_url
    }
    html = request_page(url, headers)
    if html:
        soup = BeautifulSoup(html,'lxml')
        xqs = []
        for xqitem in soup.find_all(attrs={'class': 'clear xiaoquListItem'}):
            JsonText = {
                '所在区': xqitem.find(attrs={'class': 'positionInfo'}).a.get_text(),
                '小区名': xqitem.find(attrs={'class': 'title'}).a.get_text(),
                '价格说明': xqitem.find(attrs={'class': 'priceDesc'}).get_text().strip(),
                '二手房参考均价': xqitem.find(attrs={'class': 'totalPrice'}).span.get_text(),
                '在售二手房套数': xqitem.find(attrs={'class': 'xiaoquListItemSellCount'}).a.span.get_text(),
                '小区网址': xqitem.find(attrs={'class': 'title'}).a.attrs['href']
            }
            xqs.append(JsonText)
        return xqs
    else:
        return False

    #print(xqs)

def xiaoqu_list(city_url):
    pn = 0
    xq_list = []
    while True:
        pn = pn + 1
        url = city_url + 'xiaoqu/pg%s' % pn
        print('page:', pn, '  网址：',url)
        xqs = xiaoqu_pn_list(url)
        if xqs:
            if xqs == False or pn > 500:break
            if pn == 1:
                xq_list = xqs
            else:
                if xqs[-1] == xq_list[-1] and xqs[-2] == xq_list[-2] and xqs[-3] == xq_list[-3]:break #最后页面会与上一页相同，因此作为结束标记，由于存在前面相同后面不同的反例，故取消此筛选条件，待json文档生成后删除重复项即可。
                xq_list = xq_list + xqs
        else:
            break
        time.sleep(0.5)
    #print('小区列表：',xq_list)
    return xq_list

if __name__ == '__main__':
    starttime = datetime.datetime.now()
    print('开始时间：', starttime)
    LianJia_Xq_All = []
    cities = city_url_list()
    for city_info in cities:
        province = city_info['省份']
        city = city_info['城市']
        city_url = city_info['城市网址']
        print('\n',province,'-',city)

        xiaoqu = xiaoqu_list(city_url)
        city_info['小区列表'] = xiaoqu

        LianJia_Xq_All.append(city_info)
        JsonTemp = json.dumps(LianJia_Xq_All)
        print('最新Json总表：',JsonTemp)
        print('已耗时：',round((datetime.datetime.now() - starttime).seconds/60,2),'分钟')





    db_name = '链家网爬取库'
    city_xq_colname = '链家网城市及小区数据_201909'
    city_colname = '链家网城市整体数据_201909'


    with open('%s.json' % city_xq_colname,'w') as f:
        json.dump(LianJia_Xq_All,f) #python字典数据转换为json数据

    conn = MongoClient('127.0.0.1', 27017)
    db = conn[db_name]
    collist = db.list_collection_names()
    print(collist)
    city_xq_mgdb = db[city_xq_colname]
    if city_xq_colname in collist: city_xq_mgdb.drop()
    city_xq_mgdb.insert_many(LianJia_Xq_All)


    endtime = datetime.datetime.now()
    print('开始时间：',starttime,'\n','结束时间：',endtime)
    print('共耗时：',round((endtime - starttime).seconds/60,2),'分钟')



