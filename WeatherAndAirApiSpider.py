# -*- codeing = utf-8 -*-
# @Time :2023/7/7 14:15
# @Author:云野摘星
# @File : WeatherAndAirAQISpider.py
# @Software: PyCharm
import random

import requests
from lxml import etree
import re
import datetime
import pandas as pd
import pymysql
import time
import os


def get_ip():
    inputl = 'http://www.damaiip.cn/index.php?s=/front/user/getIPlist&xsn=f79d51e050504b6a1761bf55237991c9&osn=TC_NO167004921276702283&tiqu=1'
    ip = requests.get(inputl).text
    proxy = {'http':ip}
    print(proxy)
    proxy

# 定义爬取数据的时间范围:
years = [2021,2022,2023]
months = ['01','02','03','04','05','06','07','08','09','10','11','12']

# 获取系统日期
current_year = datetime.datetime.now().year
# print(current_year)
current_month = datetime.datetime.now().month
# print(current_month)
if(current_month < 10):
    current_month = "0" + str(current_month)
else:
    current_month = str(current_month)

# 用来接收url
history_weather_urls = []
history_aqi_urls = []

# 定义一个全局变量来接收所有的历史天气及空气质量数据
all_history_weather = []
all_history_aqi = []

# 解析url 拼接出需要爬取的页面的固定式url
# http://www.tianqihoubao.com/lishi/tongling/month/202103.html
# http://www.tianqihoubao.com/lishi/tongling/month/202307.html
# 获取历史天气的url
def get_weather_url():
    # url 做拼接
    for year in years:
        for month in months:
            print(str(year)+","+str(month))
            year_month = str(year)+month
            url = 'http://www.tianqihoubao.com/lishi/tongling/month/{}.html'.format(year_month)
            history_weather_urls.append(url)
            if(year==current_year and month == current_month):
                break
            else:
                continue
    return history_weather_urls

# 获取历史aqi的url
# http://www.tianqihoubao.com/aqi/tongling-202101.html
# http://www.tianqihoubao.com/aqi/tongling-202205.html
def get_aqi_url():
    # 做url拼接
    for year in years:
        for month in months:
            year_month = str(year)+month
            url = 'http://www.tianqihoubao.com/aqi/tongling-{}.html'.format(year_month)
            history_aqi_urls.append(url)
            if(year == 2023 and month == '07'):
                break
            else:
                continue
    return history_aqi_urls

# 获取html
def get_html(url,proxy):
    # 重构请求头
    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.67'
    }
    # 发送请求 获取响应
    response = requests.get(url=url,headers=headers,proxies=proxy)

    # 获取html
    html = response.text
    print("成功获取html")

    return html

# 获取历史天气数据
def get_history_weather_data(html):
    html = etree.HTML(html)

    # 获取表格所在的位置
    trs = html.xpath('//*[@id="content"]/table//tr')

    # 遍历获取表格的数据
    for i in range(1,len(trs)):
        # 获取日期
        date = trs[i].xpath('./td[1]/a/text()')[0].strip()
        print(date)

        # 获取天气状况
        weather_condition = trs[i].xpath('./td[2]/text()')[0].strip()
        weather_condition = re.sub(r'\s','',weather_condition)
        print(weather_condition)

        # 获取最低气温 / 最高气温
        min_max_temperature = trs[i].xpath('./td[3]/text()')[0].strip()
        min_max_temperature = re.sub(r'\s', '', min_max_temperature)
        print(min_max_temperature)

        # 获取风力风向
        wind_direction = trs[i].xpath('./td[4]/text()')[0].strip()
        wind_direction = re.sub(r'\s', '', wind_direction)
        print(wind_direction)

        # 先将爬取了一天的数据放到一个list中
        one_day_weather = [
            date,
            weather_condition,
            min_max_temperature,
            wind_direction
        ]
        print(one_day_weather)

        # 再将一整天数据的列表存到 all_history_weather
        all_history_weather.append(one_day_weather)

# 获取历史aqi数据
def get_history_aqi_data(html):
    html = etree.HTML(html)

    # 获取表格所在的位置
    trs = html.xpath('//*[@id="content"]/div[3]/table/tr')

    # i = 1
    for i in range(1, len(trs)):
        # 获取日期
        aqi_date = trs[i].xpath('./td[1]/text()')[0].strip()
        print(aqi_date)

        # 获取质量等级
        air_quality_grade = trs[i].xpath('./td[2]/text()')[0].strip()
        print(air_quality_grade)

        # aqi指数
        aqi_index = trs[i].xpath('./td[3]/text()')[0].strip()
        print(aqi_index)

        # 当天AQI排名
        aqi_ranking_day = trs[i].xpath('./td[4]/text()')[0].strip()
        print(aqi_ranking_day)

        # PM2.5
        PM25 = trs[i].xpath('./td[5]/text()')[0].strip()
        # print(PM25)

        # PM10
        PM10 = trs[i].xpath('./td[6]/text()')[0].strip()
        # print(PM10)

        # So2
        So2 = trs[i].xpath('./td[7]/text()')[0].strip()
        # print(So2)

        # No2
        No2 = trs[i].xpath('./td[8]/text()')[0].strip()
        # print(No2)

        # Co
        Co = trs[i].xpath('./td[9]/text()')[0].strip()
        # print(Co)

        # O3
        O3 = trs[i].xpath('./td[10]/text()')[0].strip()
        # print(O3)

        # 将爬取一天的空气质量指数的数据放入one_day_aqi
        one_day_aqi = [
            aqi_date,
            air_quality_grade,
            aqi_index,
            aqi_ranking_day,
            PM25,
            PM10,
            So2,
            No2,
            Co,
            O3
        ]
        # print(one_day_aqi)
        # 再将每天的数据存放在all_history_aqi
        all_history_aqi.append(one_day_aqi)

# 将列表格式的所有数据转化为Pandas所支持的dataFrame
def dataFormatConcersion(cloums,data):
    # 将创建好的字段与对应的数一一匹配
    df = pd.DataFrame(data,columns=cloums)
    return df

# 创建存储天气相关的表
def createTableData(username,password,database):
    db = pymysql.connect(host='192.168.57.100', user=username, password=password, database=database)
    # 游标
    cursor = db.cursor()

    print("执行建表SQL语句")
    # 如果表存在 则删除旧表  使用execute方法 来执行SQL语句
    cursor.execute("drop table if exists history_weather")
    cursor.execute("drop table if exists history_aqi")

    # 创建一张表
    sql1="""
    create table history_weather(
            date varchar(255),
            weather_condition varchar(255),
            min_max_temperature varchar(255),
            wind_direction varchar(255)
    )character set utf8;
    """

    sql2="""
        create table history_aqi(
            aqi_date varchar(255),
            air_quality_grade varchar(255),
            aqi_index int,
            aqi_ranking_day int,
            PM25 int,
            PM10 int,
            So2 int,
            No2 int,
            Co float,
            O3 int
    )character set utf8;
    """

    # 执行sql语句
    try:
        # 先执行sql语句
        cursor.execute(sql1)
        cursor.execute(sql2)
        # 提交到数据里面执行
        db.commit()
    #抛出异常
    except Exception as e:
        # 打印报错信息
        print(repr(e))
        # 发生错误就回滚
        db.rollback()
        print("建表成功！！！")
        # 关闭数据库
        db.close()
# 将获取到的数据保存到数据库中:
def saveMysqlData(username,password,database):
    print("执行保存的SQL语句")
    db = pymysql.connect(host='192.168.57.100',user=username,password=password,database=database)

    # 游标
    cursor =db.cursor()

    # 遍历数据写到数据库
    for one in all_history_weather:
        sql1="""
        insert into history_weather(
            date,
            weather_condition,
            min_max_temperature,
            wind_direction
        ) values('%s','%s','%s','%s')
        """%(one[0],one[1],one[2],one[3])
        try:
            db.ping(reconnect=True)
            cursor.execute(sql1)
            #将其提交到数据库中执行
            db.commit()
        except Exception as e:
            print(repr(e))
            # 回滚
            db.rollback()

    for one in all_history_aqi:
        sql2="""
        insert into history_aqi(
            aqi_date,
            air_quality_grade,
            aqi_index,
            aqi_ranking_day,
            PM25,
            PM10,
            So2,
            No2,
            Co,
            O3
        ) values('%s','%s','%d','%d','%d','%d','%d','%d','%f','%d')
        """%(one[0],one[1],int(one[2]),int(one[3]),int(one[4]),int(one[5]),int(one[6]),int(one[7]),float(one[8]),int(one[9]))

        # 执行sql语句
        try:
            db.ping(reconnect=True)
            cursor.execute(sql2)
            #将其提交至数据库中运行
            db.commit()
        except Exception as e:
            print(repr(e))
            db.rollback()
    print("插入数据成功")
    db.close()


# 调用函数
if __name__ == '__main__':
    proix = get_ip()
    # 处理天气数据
    weather_urls = get_weather_url()
    for weather_url in weather_urls:
        weather_html = get_html(weather_url,proix)
        get_history_weather_data(weather_html)
        time.sleep(random.randrange(1,2))


    proix = get_ip()
    # 处理aqi数据
    aqi_urls = get_aqi_url()
    for aqi_url in aqi_urls:
        aqi_html = get_html(aqi_url,proix)
        get_history_aqi_data(aqi_html)
        time.sleep(random.randrange(1, 2))

    # 将数据写道数据库中去
    # 首先定义Mysql的相关信息
    username = "root"
    password = "123456"
    database = "tongling_weather_1"

    #创建表
    createTableData(username=username,password=password,database=database)

    # 插入数据到表
    saveMysqlData(username=username,password=password,database=database)
    print("数据抓取结束！！！")
