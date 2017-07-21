#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 17:53:28 2017
帮助函数文档
@author: lywen
"""
import datetime as dt
import math
def get_quarters(begin,end):
    """
    获取指定日期内的季节
    @@parm:begin:开始时间begin=(2016,1)
    @@parm:end:结束时间end=(2017,1)
    """
    if begin is None:
        """最早默认时间"""
        begin = (1990,1)
    if end is None  :
        now = dt.datetime.now()
        year = now.year()
        quarter = int(math.ceil(now.month/3.0))
        end = (year,quarter)
        
    
        
        
    

