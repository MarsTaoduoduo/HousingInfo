import requests
from bs4 import BeautifulSoup
import re,time,json,datetime
from pymongo import MongoClient
import pandas as pd


# 请求网页
#----------------------------------------------------------------------------------------
def request_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
        'Referer': 'https://sz.lianjia.com/'
    }
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
def get_city_infos(begin_url):
    html = request_page(begin_url)
    soup = BeautifulSoup(html,'lxml')
    city_infos = []
    for province in soup.find_all(attrs={'class': 'city_province'}):
        for city in province.find_all(name='a'):
           info = {
               '省份': province.div.get_text(),
               '城市': city.get_text(),
               '城市网址': city.attrs['href']
           }
           city_infos.append(info)
    return city_infos

# 获取单个城市全部小区信息
#----------------------------------------------------------------------------------------
def get_pn_xq_infos(city_info,pn_xq_url):
    html = request_page(pn_xq_url)
    if html:
        soup = BeautifulSoup(html,'lxml')
        pn_xq_infos = []
        for xqitem in soup.find_all(attrs={'class': 'clear xiaoquListItem'}):
            info = {
                '小区所在区': xqitem.find(attrs={'class': 'positionInfo'}).a.get_text(),
                '小区名': xqitem.find(attrs={'class': 'title'}).a.get_text(),
                '小区价格说明': xqitem.find(attrs={'class': 'priceDesc'}).get_text().strip(),
                '小区二手房参考均价': xqitem.find(attrs={'class': 'totalPrice'}).span.get_text(),
                '小区在售二手房套数': xqitem.find(attrs={'class': 'xiaoquListItemSellCount'}).a.span.get_text(),
                '小区网址': xqitem.find(attrs={'class': 'title'}).a.attrs['href']
            }
            info.update(city_info)
            pn_xq_infos.append(info)
        return pn_xq_infos
    else:
        return False

def get_xq_infos(city_info):
    pn = 0
    xq_infos = []
    city_url = city_info['城市网址']
    while True:
        pn = pn + 1
        pn_xq_url = city_url + 'xiaoqu/pg%s' % pn
        print('page:', pn, '  网址：',pn_xq_url)
        pn_xq_infos = get_pn_xq_infos(city_info,pn_xq_url)
        if pn_xq_infos:
            if pn_xq_infos == False or pn > 500:break
            if pn == 1:
                xq_infos = pn_xq_infos
            else:
                if pn_xq_infos[-1] == xq_infos[-1] and pn_xq_infos[-2] == xq_infos[-2] and pn_xq_infos[-3] == xq_infos[-3]:break #最后页面会与上一页相同，因此作为结束标记，由于存在前面相同后面不同的反例，故取消此筛选条件，待json文档生成后删除重复项即可。
                xq_infos = xq_infos + pn_xq_infos
        else:
            break
        time.sleep(0.5)
    #print('小区列表：',xq_list)
    return xq_infos

# 获取单个小区已成交的全部房屋信息
#----------------------------------------------------------------------------------------
def get_fw_infos(xq_info):
    xq_url = xq_info['小区网址']
    html = request_page(xq_url)
    fw_infos = []
    pn = 0
    if html:
        soup = BeautifulSoup(html,'lxml')
        try:
            fw_url = soup.find(attrs={'class': 'btn-large'}).attrs['href']
            while True:
                pn = pn + 1
                print('小区名：',xq_info['小区名'],'    第%d页' % pn)
                pn_fw_url = fw_url.replace('chengjiao/','chengjiao/pg%d' % pn)
                pn_fw_infos = get_pn_fw_infos(xq_info, pn_fw_url)
                if pn_fw_infos == []:
                    break
                    print('本页无数据，小区二手房交易数据收集结束')
                fw_infos = fw_infos + pn_fw_infos
                print('小区网址：', pn_fw_url, '\n整理二手房交易数据：', pn_fw_infos,'\n小区房屋数据量为：',len(fw_infos))
        except:
           print(xq_info['小区名'], '无已成交二手房，小区网址为：', xq_info['小区网址'])
           pass
        return fw_infos

    else:
        print('未能连接到',xq_info['小区名'],'的网址：',xq_info['小区网址'])
        return None


def get_pn_fw_infos(xq_info, pn_fw_url):
    html = request_page(pn_fw_url)
    if html:
        soup = BeautifulSoup(html,'lxml')
        pn_fw_infos = []
        for ershou_deal in soup.find_all(attrs={'class': 'info'}):
            info = {
                '交易二手房名': ershou_deal.find(attrs={'class': 'title'}).a.get_text().strip(),
                '交易总价':ershou_deal.find(attrs={'class': 'totalPrice'}).get_text().strip(),
                '交易单价': ershou_deal.find(attrs={'class': 'unitPrice'}).get_text().strip(),
                '成交日期':ershou_deal.find(attrs={'class': 'dealDate'}).get_text().strip(),
                '交易二手房详情网址':ershou_deal.find(attrs={'class': 'title'}).a.attrs['href']
            }
            info.update(xq_info)
            detail_html = request_page(info['交易二手房详情网址'])
            if detail_html:
                soup2 = BeautifulSoup(detail_html, 'lxml')
                for detail_cont in soup2.find_all(attrs={'class': 'content'}):
                    for detail_li in detail_cont.find_all(name='li'):
                        info[detail_li.span.get_text()] = detail_li.get_text().replace(detail_li.span.get_text(),'').strip()
            pn_fw_infos.append(info)
        return pn_fw_infos
    else:
        return False


# 在MongoDB中保存结果
#----------------------------------------------------------------------------------------
def save_to_MongoDB(list,dbname,colname):
    conn = MongoClient('127.0.0.1', 27017)
    db = conn[dbname]
    collist = db.list_collection_names()
    tech_mgdb = db[colname]
    if colname in collist: tech_mgdb.drop()
    tech_mgdb.insert_many(list)


# 主要程序
#----------------------------------------------------------------------------------------
def get_raw_data():
    raw_data = []
    xq_infos_all = []
    begin_url = 'https://www.lianjia.com/city/'

    city_infos = get_city_infos(begin_url)
    save_to_MongoDB(city_infos, 'LianjiaDB', 'city_data')


    for city_info in city_infos:
        if city_info['城市'] in ['北京','上海','深圳','广州','杭州','成都','重庆']:
            del city_info['_id'] #避免由于重复id导致存入MongoDB时报错
            print('城市：',city_info['城市'],'\n','*'*100)
            xq_infos = get_xq_infos(city_info)
            xq_infos_all = xq_infos_all + xq_infos
            save_to_MongoDB(xq_infos_all, 'LianjiaDB', 'xq_data')

            for xq_info in xq_infos:
                del xq_info['_id']
                print('小区：',xq_info['小区名'],xq_info['小区网址'],'\n','-'*20)
                fw_infos = get_fw_infos(xq_info)
                print('\n\n',xq_info['小区名'], '二手房成交的房屋信息如下：',fw_infos, '\n','-'*20,)
                raw_data = raw_data + fw_infos
                if raw_data:
                    save_to_MongoDB(raw_data, 'LianjiaDB', 'raw_data')






# 执行主要程序
#----------------------------------------------------------------------------------------

if __name__ == '__main__':
    get_raw_data()

