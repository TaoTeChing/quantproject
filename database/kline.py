#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-09-14 21:22:13
# Project: kline
#reated on 2016-09-14 21:22:13
# Project: kline



from pyspider.libs.base_handler import *
from bs4 import BeautifulSoup
from pyspider.database.mongodb.resultdb import ResultDB  
from pyspider.database.mongodb import resultdb

from pymongo import MongoClient

import pymongo
import datetime as dt
import re

import traceback
import xmltodict

class Handler(BaseHandler):
    Now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    crawl_config = {
    }
    codes = """
            <code>
             <AUDCAD>澳元加元</AUDCAD>
            <AUDCHF>澳元瑞郎</AUDCHF>
            <AUDEUR>澳元欧元</AUDEUR>
            <AUDHKD>澳元港元</AUDHKD>
            <AUDJPY>澳元日元</AUDJPY>
            <AUDUSD>澳元美元</AUDUSD>
            <CADHKD>加元港元</CADHKD>
            <CADJPY>加元日元</CADJPY>
            <CHFCAD>瑞郎加元</CHFCAD>
            <CHFHKD>瑞郎港元</CHFHKD>
            <CHFJPY>瑞郎日元</CHFJPY>
            <EURAUD>欧元澳元</EURAUD>
            <EURCAD>欧元加元</EURCAD>
            <EURCHF>欧元瑞郎</EURCHF>
            <EURGBP>欧元英镑</EURGBP>
            <EURHKD>欧元港元</EURHKD>
            <EURJPY>欧元日元</EURJPY>
            <EURUSD>欧元美元</EURUSD>
            <GBPAUD>英镑澳元</GBPAUD>
            <GBPCAD>英镑加元</GBPCAD>
            <GBPCHF>英镑瑞郎</GBPCHF>
            <GBPHKD>英镑港元</GBPHKD>
            <GBPJPY>英镑日元</GBPJPY>
            <GBPUSD>英镑美元</GBPUSD>
            <HKDJPY>港元日元</HKDJPY>
            <USDCAD>美元加元</USDCAD>
            <USDCHF>美元瑞郎</USDCHF>
            <USDHKD>美元港元</USDHKD>
            <USDJPY>美元日元</USDJPY>
            <NZDAUD>新西兰元澳元</NZDAUD>
            <NZDCAD>新西兰元加元</NZDCAD>
            <NZDCHF>新西兰元瑞郎</NZDCHF>
            <NZDEUR>新西兰元欧元</NZDEUR>
            <NZDHKD>新西兰元港元</NZDHKD>
            <NZDJPY>新西兰元日元</NZDJPY>
            <USDX>美元指数</USDX>
            <XAGEUR>白银欧元</XAGEUR>
            <XAGGBP>白银英镑</XAGGBP>
            <XAGUSD>白银美元</XAGUSD>
            <XAUAUD>黄金澳元</XAUAUD>
            <XAUEUR>黄金欧元</XAUEUR>
            <XAUGBP>黄金英镑</XAUGBP>
            <XAUUSD>黄金美元</XAUUSD>
            <USDRUB>美元俄罗斯卢布</USDRUB>
            <USDBRL>美元巴西雷亚尔</USDBRL>
            <USDIDR>美元印尼盾</USDIDR>
            <USDINR>美元印度卢比</USDINR>
            <USDKRW>美元韩元</USDKRW>
            <USDMXN>美元墨西哥比索</USDMXN>
            <USDNOK>美元挪威克朗</USDNOK>
            <USDSAR>美元沙特里亚尔</USDSAR>
            <USDTRY>美元土耳其里拉</USDTRY>
            <TWDCNY>新台币人民币</TWDCNY>
            <HKDCNY>港币人民币</HKDCNY>
            <EURCNY>欧元人民币</EURCNY>
            <GBPCNY>英镑人民币</GBPCNY>
            <CHFCNY>瑞郎人民币</CHFCNY>
            <SGDCNY>新加坡元人民币</SGDCNY>
            <NZDCNY>新西兰元人民币</NZDCNY>
            <AUDCNY>澳元人民币</AUDCNY>
            <CADCNY>加元人民币</CADCNY>
            <CNYJPY>人民币日元</CNYJPY>
            <USDCNY>美元人民币</USDCNY>
            <KRWCNY>韩元人民币</KRWCNY>
            <RUBCNY>俄罗斯卢布人民币</RUBCNY>
            <THBCNY>泰铢人民币</THBCNY>
            <CNHCNY>离岸人民币人民币</CNHCNY>
            <CNHCNY>离岸人民币人民币</CNHCNY>
            <CNHXAU>离岸人民币黄金</CNHXAU>
            <CNHXAG>离岸人民币白银</CNHXAG>
            <USDCNH>离岸人民币</USDCNH>
            </code>"""
    types = """<type>
                 
                 <minute_1>0</minute_1>
                 <minute_5>1</minute_5>
                 <minute_15>2</minute_15>
                 <minute_30>3</minute_30>
                 <minute_60>4</minute_60>
                 <day>5</day>
                 <week>6</week>
                 <month>9</month>
                  <quarter>12</quarter>
            </type>"""
    typesvalues = xmltodict.parse(types)['type'].values()
    codesdict = xmltodict.parse(codes)['code']
    @every(minutes=5 )##设定定时任务，每5分钟执行一次
    def on_start(self):
        """
        爬取外汇汇率
        """
        start = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        number = '-10'
        for code in self.codesdict.keys():
            for types in self.typesvalues:

                url = 'http://webforex.hermes.hexun.com/forex/kline?code=FOREX%s&start=%s&number=%s&type=%s'%(code,start,number,types)
                
                #if int(types) in [0,1,2]:
                #    age = 5 ## 5min执行一次
                #elif int(types)==3:
                #    age=30 ## 30min执行一次
                    
                #elif int(types)==4:
                #    age=60 ## 60min执行一次
                #elif int(types)>=5:
                #    age=60*24 ## 1天执行一次
                self.crawl(url, callback=self.index_page, priority =int(types))    
                #self.crawl(url, callback=self.index_page,age=age*60, auto_recrawl=True, priority =int(types))
        

    #@config(age=5 * 60)### age执行有效期
    def index_page(self, response):
        """
        
        """
        text = eval(response.text.replace(';',''))
        Data = text.get('Data')
        #return text
        Kline = text.get('KLine')
        data = None
        code = re.findall('code=(.*)&start',response.url)[0][5:]
        types = re.findall('type=(.*)',response.url)[0]
        if Kline is not None and Data is not None:
            colname = map(lambda x:x.keys()[0],Kline)+['PriceWeight','code','type','des']
            data =  map(lambda x:dict(zip(colname,x+[Data[-1],code,types,self.codesdict.get(code)])),Data[0] )
        datawh = []
        for lst in data:
            lst.update({'Time':str(lst.get('Time'))})
            datawh.append(({'code':lst.get('code'),
                            'Time':lst.get('Time'),
                            'type':lst.get('type')
                            },##where 
                           
                            {'Close':lst.get('Close'),
                            'High':lst.get('High'),
                            'Low':lst.get('Low'),
                            'Close':lst.get('Close'),
                            'Open':lst.get('Open'),
                            'updatetime':self.Now,
                            'Amount':lst.get('Amount'),
                            'Volume':lst.get('Volume'),
                            'PriceWeight':lst.get('PriceWeight'),
                            'des':lst.get('des')
                            }##set
                           ))
            #lst.update({'updatetime':self.Now})
        return datawh
        
    def on_result(self, result):
            sql = ResultDBs(self.project_name)
            sql.save(result)


class ResultDBs():
    

    def __init__(self, project,database='resultdb'):
        url = 'mongodb://resultdb:resultdb@127.0.0.1:27017/resultdb'
        self.conn = MongoClient(url)
        self.conn.admin.command("ismaster")
        self.database = self.conn[database]
        self.project =project
        
        if project not in self.database.collection_names():
            self._create_project(project,indexlist=["code",'type','Time'])
            
        self.collection_name = self.database[project]
        
        

    def _create_project(self, project,indexlist=["curreny","code"]):
        collection_name = self.database[project]
        indexs = map(lambda x:(x,pymongo.ASCENDING),indexlist)
        collection_name.create_index(indexs,unique=True)
        

    def save(self, result):
        """
        更新collectname 给定条件的数据
        data =[(element,wh)]
        element:更新数据列表
        wh:更新条件列表
        example:
               element = {'a':1,'b':3}
               wh = {'c':3}
               
        """

        if result is not None:
            collection = self.collection_name
            for wh,element in result:
                try:
                   
                   collection.update_many(wh,{'$set': element},upsert =True)
                except:
                   traceback.print_exc()
                   

                


