#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 16:20:45 2017
tasks
@author: lywen
"""

import time
from celery import Celery
backend = 'redis://localhost:6379/1'
broker = 'redis://localhost:6379/0'

celery = Celery('tasks', broker=broker,backend=backend)

@celery.task
def sendmail(mail):
    print('sending mail to %s...' % mail['to'])
    time.sleep(2.0)
    print('mail sent.')
    
    
@celery.task
def add(a,b):
    print('sending mail to...')
    
    print('mail sent.{}'.format(a+b))
    return  a+b

if __name__ == '__main__':
    celery.worker_main()