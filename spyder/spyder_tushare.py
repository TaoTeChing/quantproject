#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 14:12:38 2017
运用tushare 获取
@author: lywen
历史行情数据
复权历史数据
实时行情数据
历史分笔数据
实时报价数据
当日历史分笔
大盘指数列表
大单交易数据

"""
import tushare as ts
import datetime as dt
import sys
sys.path.append('..')
from database.mongo import ResultDBs
from database.mongodb import mongodb
import pandas as pd        
import traceback          
import math      
from help.date import get_quarters 

class database(object):
    """
    存储数据
    """
    def __init__(self,data=None,indexList=None,tableName = None,save=False):
        self.data = data
        self.indexList = indexList
        self.tableName = tableName
        if save:
            self.save()
        
    def save(self):
        if self.data is not None:
            data = self.data.to_dict('records')
            indexList = self.indexList
            tableName =self.tableName
            db = ResultDBs(tableName,indexList)
            db.save(data)
            
        
            

class SymbolMdm(object):
    """
    股票基础数据，存储到dataframe中
    """
    def __init__(self,save=True):
        """
        code,代码
        name,名称
        industry,所属行业
        area,地区
        pe,市盈率
        outstanding,流通股本(亿)
        totals,总股本(亿)
        totalAssets,总资产(万)
        liquidAssets,流动资产
        fixedAssets,固定资产
        reserved,公积金
        reservedPerShare,每股公积金
        esp,每股收益
        bvps,每股净资
        pb,市净率
        timeToMarket,上市日期
        undp,未分利润
        perundp, 每股未分配
        rev,收入同比(%)
        profit,利润同比(%)
        gpr,毛利率(%)
        npr,净利润率(%)
        holders,股东人数
        datatime:获取数据日期
        datatimestramp:获取数据时间戳
        """
        
        try:
            stockBasics = ts.get_stock_basics()
        except:
            stockBasics  = None
            
        if  stockBasics  is not None:
            now = dt.datetime.now()
            stockBasics['datatime'] = now.strftime('%Y-%m-%d')
            stockBasics['datatimestramp'] = now.strftime('%H:%M:%S')
            stockBasics['code'] = stockBasics.index
            indexlist = ['code','datatime']##数据库索引
            tableName = 'SymbolMdm'
            print self.__name__
            database(stockBasics,indexlist,tableName,save)

        
        

class companyRepor(object):
    """
    公司业绩报告
    code,代码
    name,名称
    esp,每股收益
    eps_yoy,每股收益同比(%)
    bvps,每股净资产
    roe,净资产收益率(%)
    epcf,每股现金流量(元)
    net_profits,净利润(万元)
    profits_yoy,净利润同比(%)
    distrib,分配方案
    report_date,发布日期
    """
    def __init__(self,year=None,quarter=None,save=True,updateAll=False,beginyear=1990,beginquarter=1):
        """
        获取季度的公司业绩报告
        @@parm:year:年份
        @@parm:quarter:季度
        @@parm:updateAll是否全量更新历史所有数据
        """
        now = dt.datetime.now()
        if year is None or quarter is None:
                year = now.year
                quarter = math.ceil(now.month/3.0)
                
        if not updateAll:
            
            try:
               profitData  = ts.get_report_data(year,quarter)
               profitData['datatime']  =  now.strftime('%Y-%m-%d')
               profitData['datatimestramp']  =  now.strftime('%H:%M:%S')
               profitData['year'] = year    
               profitData['quarter'] = quarter
               indexlist = ['code','year','quarter']##数据库索引
               tableName = 'companyRepor'  
               database(profitData,indexlist,tableName,save)
               
            except :
                traceback.print_exc()
                
        else:
            ##生成季度时间序列
            quarters = get_quarters((beginyear,beginquarter),(year,quarter))
            for y,q in quarters:
                companyRepor(y,q)##递归获取所有历史数据
            
class HistData(object):
    """
    获取股票历史数据
    date：日期
    open：开盘价
    high：最高价
    close：收盘价
    low：最低价
    volume：成交量
    price_change：价格变动
    p_change：涨跌幅
    ma5：5日均价
    ma10：10日均价
    ma20:20日均价
    v_ma5:5日均量
    v_ma10:10日均量
    v_ma20:20日均量
    turnover:换手率[注：指数无此项]
    """
    def __init__(self,code=None,start=None,end=None,save=True):
        """
        get symbol history data
        @@param:code:股票代码，如果code is None,或者全部股票
        @@parm:start:开始时间
        @@end:结束时间
        """
        now = dt.datetime.now()
        if end is None:
            end = (now -dt.timedelta(1)).strftime('%Y-%m-%d')
        if start is None:
            start = '1990-01-01'
            
        if code is None:
            mongo =mongodb()##获取股票代码
            collectname = 'SymbolMdm'
            where = {}
            kwargs = {'code':1}##获取code代码字断
            codes = mongo.select(collectname,where,kwargs)
            for code in codes:
                HistData(code.get('code'),HistData,end)##递归请求历史数据
        else:
            try:
                data = ts.get_hist_data(code,start=start,end=end) 
                data['datatime']  =  now.strftime('%Y-%m-%d')
                data['datatimestramp']  =  now.strftime('%H:%M:%S')
                data['date'] =data.index
                data['code'] = code
                indexlist = ['code','date']##数据库索引
                tableName = 'HistData'  
                database(data,indexlist,tableName,save)
                print 'info:{} downloaded is ok,num:{}!       '.format(code,len(data))
            except:
                pass
                    

class symbClassified(object):
    """
    行业分类
    概念分类
    地域分类
    中小板分类
    创业板分类
    风险警示板分类
    沪深300成份股及权重
    上证50成份股
    中证500成份股
    终止上市股票列表
    暂停上市股票列表
    """
    def __init__(self):
        """
        """
        self.columns = ['code',
                   'name',
                   'industryName',##行业名称
                   'conceptName',##概念分类
                   'areaName',##地域分类
                   'smeName',##是否属于中小板分类
                   'gemName',##是否属于创业板分类
                   'stName',##是否属于警示板分类
                   'hs300s',##是否是沪深300当前成份股及所占权重
                   'sz50s',##获取上证50成份股
                   'zz500s',##中证500成份股
                   'terminated',##终止上市股票列表
                   'suspended',##暂停上市股票列表
                   ]
        self.data = None
        self.get_industry_classified()
        self.get_concept_classified()
        self.get_area_classified()
        self.get_sme_classified()
        self.get_gem_classified()
        self.get_st_classified()
        self.get_hs300s_classified()
        self.get_sz50s_classified()
        self.get_zz500s_classified()
        self.get_terminated_classified()
        self.get_suspended_classified()
        now = dt.datetime.now()
        self.data['datatime']  =  now.strftime('%Y-%m-%d')
        self.data['datatimestramp']  =  now.strftime('%H:%M:%S')
        
        
    def get_industry_classified(self):
        """
        行业分类
         code    name      c_name
        """
        
        industry = ts.get_industry_classified()
        industry = industry[['code','name','c_name']]
        industry.columns = ['code','name','industryName']
        if self.data is None:
            self.data = industry
        
    def get_concept_classified(self):
        """
        概念分类分类
        """
        concept = ts.get_concept_classified()
        concept = concept[['code','c_name']]
        concept.columns = ['code','conceptName']
        self.data = pd.merge(self.data,concept,on=['code'],how='outer')
        
    def get_area_classified(self):
        """
        地域分类
        """
        area = ts.get_area_classified()
        area = area[['code','area']]
        area.columns = ['code','areaName']
        self.data = pd.merge(self.data,area,on=['code'],how='outer')
    
    def get_sme_classified(self):
        """中小板分类"""
        sme = ts.get_sme_classified()
        
        sme['smeName'] = 1
        sme = sme[['code','smeName']]
        sme.columns = ['code','smeName']
        self.data = pd.merge(self.data,sme,on=['code'],how='outer')
        
    def get_gem_classified(self):
        """
        创业板分类
        """
        gem = ts.get_gem_classified()
        
        gem['gemName'] = 1
        gem = gem[['code','gemName']]
        gem.columns = ['code','gemName']
        self.data = pd.merge(self.data,gem,on=['code'],how='outer')
        
    def get_st_classified(self):
        """
        是否属于警示板分类
        """
        st = ts.get_st_classified()
        
        st['stName'] = 1
        st = st[['code','stName']]
        st.columns = ['code','stName']
        self.data = pd.merge(self.data,st,on=['code'],how='outer')
        
    def get_hs300s_classified(self):
        """
        'hs300s',##是否是沪深300当前成份股及所占权重
        """
        hs300 = ts.get_hs300s()
        hs300['hs300sName'] = 1
        hs300 = hs300[['code','hs300sName','date', 'weight']]
        
        hs300.columns = ['code','hs300sName','hs300sDate', 'hs300sWeight']
        self.data = pd.merge(self.data,hs300,on=['code'],how='outer')
        
    def get_sz50s_classified(self):
        """
        'sz50s',##获取上证50成份股
        """
        sz50s = ts.get_sz50s()
        sz50s['sz50sName'] = 1
        sz50s = sz50s[['code','sz50sName']]
        
        sz50s.columns = ['code','sz50sName']
        self.data = pd.merge(self.data,sz50s,on=['code'],how='outer')    
  
    def get_zz500s_classified(self):
        """
        'zz500s',##中证500成份股
        """
        zz500s = ts.get_zz500s()
        zz500s['zz500sName'] = 1
        zz500s = zz500s[['code','zz500sName','date','weight']]
        
        zz500s.columns = ['code','zz500sName','zz500sDate','zz500sWeight']
        self.data = pd.merge(self.data,zz500s,on=['code'],how='outer')
        
    def get_terminated_classified(self):
        """
        'terminated',##终止上市股票列表
        """
        terminated = ts.get_terminated()
        terminated['terminatedName'] = 1
        terminated = terminated[['code','terminatedName','oDate','tDate']]
        
        terminated.columns = ['code','terminatedName','terminatedODate','terminatedTDate']
        self.data = pd.merge(self.data,terminated,on=['code'],how='outer')
        
    def get_suspended_classified(self):
        """
       'suspended',##暂停上市股票列表
        """
        suspended = ts.get_suspended()
        suspended['suspendedName'] = 1
        suspended = suspended[['code','suspendedName','oDate','tDate']]
        
        suspended.columns = ['code','suspendedName','suspendedODate','suspendedTDate']
        self.data = pd.merge(self.data,suspended,on=['code'],how='outer')  
        
    
        
    
if __name__== '__main__':
    
    #mdm = SymbolMdm()##股票基础数据
    #companyRepor(2017,2,updateAll=True)##公司业绩报告
    ##HistData()##历史数据
    pass
    
    

