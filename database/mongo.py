#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#created on 2016-09-21 17:34:21
# Project: market
#@author: lywen

from pymongo import MongoClient
from config import get_mongo_config ##database config

import pymongo
import traceback
def splitwhereset(data,index=None):
    """
    according the index split the data
    if index=None:
    return data
    else:
    return
    [where,set]
    """
    
    if index is None or data is None:
        return data
    else:
        wheredata = []
        for lst in data:
            where = {}
            for ind in index:
                where[ind] = lst.get(ind)
                lst.pop(ind)
            sets = lst
            wheredata.append((where,sets))
                
        return wheredata


class ResultDBs():
    """
    自定义mongo接口，实现批量写入将数据写入数据库
    """

    def __init__(self, project,indexlist):
        """
        url:database url
        project:collectname
        indexlist:collect index
        """
        user,password,host,port,db = get_mongo_config()
        url = 'mongodb://{}:{}@{}:{}/{}'.format(user,password,host,port,db )
        #url = 'mongodb://resultdb:resultdb@127.0.0.1:27017/resultdb'
        database = url.split('/')[-1]
        self.index = indexlist
        self.conn = MongoClient(url)
        self.conn.admin.command("ismaster")
        self.database = self.conn[database]
        self.project =project
        
        if project not in self.database.collection_names():
            self._create_project(project,indexlist=indexlist)
            
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
        
        obj = splitwhereset(result,self.index)

        if obj is not None:
            collection = self.collection_name
            for wh,element in obj:
                try:
                   
                   collection.update_many(wh,{'$set': element},upsert =True)
                except:
                   traceback.print_exc()
            self.conn.close()


