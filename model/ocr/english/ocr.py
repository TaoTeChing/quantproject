#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 15:54:56 2017
crnn  
@@ model url  https://github.com/bgshih/crnn and  https://github.com/meijieru/crnn.pytorch
    author    = {Baoguang Shi and
                   Xiang Bai and
                   Cong Yao},
      title     = {An End-to-End Trainable Neural Network for Image-based Sequence Recognition
                   and Its Application to Scene Text Recognition},
      journal   = {CoRR},
      volume    = {abs/1507.05717},
      year      = {2015}
    }
@author: lywen
"""

import torch
from torch.autograd import Variable
import utils
import dataset


import crnn as crnn

modelPath = 'model/ocr/english/crnn.pth'

alphabet = '0123456789abcdefghijklmnopqrstuvwxyz'

if torch.cuda.is_available():
    model = crnn.CRNN(32, 1, 37, 256, 1).cuda()
else:
    model = crnn.CRNN(32, 1, 37, 256, 1).cpu()
### load model weigth from path 
model.load_state_dict(torch.load(modelPath))

converter = utils.strLabelConverter(alphabet)

transformer = dataset.resizeNormalize((100, 32))

def crnn_ocr(image):
    image = image.convert('L')
    if torch.cuda.is_available():
        image = transformer(image).cuda() 
    else:
        image = transformer(image).cpu()
    image = image.view(1, *image.size())
    image = Variable(image)
    model.eval()
    preds = model(image)
    _, preds = preds.max(2)
    preds = preds.squeeze(2)
    preds = preds.transpose(1, 0).contiguous().view(-1)
    preds_size = Variable(torch.IntTensor([preds.size(0)]))
    sim_pred = converter.decode(preds.data, preds_size.data, raw=False)
    return sim_pred
    
