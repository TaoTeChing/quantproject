#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 18:01:43 2017
help datetime 
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
        
    beginQuarter = begin
    endQuarter = int('%04d%02d'%(end[0],end[1]))
    Quarters = []
    while 1:
        year    = beginQuarter[0]
        quarter = beginQuarter[1]
        yearQuarter = int('%04d%02d'%(year,quarter))
        if yearQuarter<= endQuarter:
            
            Quarters.append(beginQuarter)
            if quarter+1>4:
                beginQuarter = (year+1,1)
            else:
                 beginQuarter = (year,quarter+1)
        else:
            break
        
    return Quarters
        
        

