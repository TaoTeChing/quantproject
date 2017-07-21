#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 14:26:40 2017
database config
@author: lywen
use quant
db.createUser({'user':'quant','pwd':'quant','roles':[{'role':'readWrite','db':'quant'}]})
"""
def get_mongo_config():
    """获取mongo配置"""
    user = 'quant'
    password ='quant'
    host='127.0.0.1'
    port=27017
    db='quant'
    return user,password,host,port,db



