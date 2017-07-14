#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 15:00:08 2017
运用 谷歌tesseract\crnn识别常见的验证码
@author: lywen
"""
from __future__ import print_function 
from  pytesseract import image_to_string
import six
import urllib2
import numpy as np
import os
try:
   from ocr.english.ocr import crnn_ocr
except:
   print("pytorch don't install,learn more from https://github.com/meijieru/crnn.pytorch" )

try :
   import cv2 
except:
    print("opencv don't install!")
from PIL import Image

def tesseract(image,lang):
    """
    pytesseract ocr
    """
    return image_to_string(image, lang=lang, config='--psm 7')

def crnn(image):
    """
    deep learning model crnn from chinses and  english verification
    current  Unwanted crnn
    @article{ShiBY15,
    @@ model url  https://github.com/bgshih/crnn
    author    = {Baoguang Shi and
                   Xiang Bai and
                   Cong Yao},
      title     = {An End-to-End Trainable Neural Network for Image-based Sequence Recognition
                   and Its Application to Scene Text Recognition},
      journal   = {CoRR},
      volume    = {abs/1507.05717},
      year      = {2015}
    }
    """
    return crnn_ocr(image)
import traceback
        
def verification(url=None,path=None,lang='chi_sim',clean=False,engine='pytesseract'):
    """
    tesseract ocr chinses\english verification
    @@param:url ,if url is not None,it will get image from url with function read_url_img
    @@oarm:path,if path is not None,it with get image from file
    @@param:lang,language choose in ['chi_sim','eng','chi_sim']
    @@param:clean,whether simple clean ths image
    @@ engine:pytesseract,crnn engine
    @@return :uft-8 string
    """
    if url is not None:
        image = read_url_img(url,decode=True)
    if path is not None:
        if os.path.exists(path):
           try:
             image = Image.open(path)
           except:
             traceback.print_exc()
             return ''
        else:
             return ''
    if clean:
        
        img = np.array(image.convert('L'))
        meanImg = img.mean()
        img[img>meanImg] = 255
        img[img<=meanImg] = 0
        image = Image.fromarray(img)
        
    if engine=='pytesseract':
        
        return tesseract(image, lang)
    else:
        return  crnn(image)
    
       

    

def check_image_is_valid(imageBin):
    """
    check image valid
    """
    if imageBin is None:
        return False
    imageBuf = np.fromstring(imageBin, dtype=np.uint8)
    
    img = cv2.imdecode(imageBuf, cv2.IMREAD_GRAYSCALE)
    if img is None:
        return False
    
    imgH, imgW = img.shape[0], img.shape[1]
    if imgH * imgW == 0:
        return False
    return True


def read_url_img(url,decode=True):
    """
    get image from url with urllib2
    """
    try:
        req = urllib2.urlopen(url)
        imgString = req.read()
        if check_image_is_valid(imgString):
            if decode:
                
                buf = six.BytesIO()
                buf.write(imgString)
                buf.seek(0)
                img = Image.open(buf)
                
                return img
            else:
                imgString
        else:
            return None
    except:
        return None
    

    