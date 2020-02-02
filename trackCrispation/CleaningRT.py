#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json
import pickle
import os
import itertools
from ast import literal_eval
import numpy as np
import sys
from datetime import datetime
from scipy import stats
from IPython.display import clear_output, display
import subprocess
from datetime import datetime
pd.options.display.float_format = '{:.0f}'.format


# In[2]:


def LoadJsonFile(filename): 
    with open(filename, 'r') as f:
        DicConfig = json.load(f)
    return DicConfig


def GlobalDicDeplier(OneDic):
    for k,v in OneDic.items():
        exec('globals()[k] = v')
    return None


# In[3]:


DicConfig = LoadJsonFile(os.path.join(os.getcwd(),"config.json"))
GlobalDicDeplier(DicConfig)
sys.path.append(Root)
from fun import *

print("Load Config variables")


# In[4]:


print("Loading data ...")
# LOAD DATA
path = os.path.join(Root,FolderProject,"RefRT.pkl")
RefRT = LoadPickleOrInit(path)
ListOfTMResults = PickleLoad(os.path.join(Root,FolderProject,"SequenceOfGraphResults.pkl"))


# In[5]:


print("Analytics ...")
# ANALYTICS
LastTimestamp = max([v["TimeMark"] for k,v in ListOfTMResults.items()])
CleanRTDF = RefRT.copy()[RefRT.TWEETUNIXEPOCH>LastTimestamp]


# In[8]:


print("Save new RefRT ...")
# SAVE
PickleDump(os.path.join(Root,FolderProject,"RefRTSaved.pkl"),CleanRTDF)


# In[9]:


print("Writing Logs ...")
# LOGS
LogNoldRTDF = len(RefRT)
LogNnewRTDF = len(CleanRTDF)

LogFirstDateOld = str(pd.to_datetime(RefRT.TWEETUNIXEPOCH.min(),unit="s"))
LogLastDateOld = str(pd.to_datetime(RefRT.TWEETUNIXEPOCH.max(),unit="s"))

LogFirstDateNew = str(pd.to_datetime(CleanRTDF.TWEETUNIXEPOCH.min(),unit="s"))
LogLastDateNew = str(pd.to_datetime(CleanRTDF.TWEETUNIXEPOCH.max(),unit="s"))

log ={"NoldRTDF":LogNoldRTDF,
      "NnewRTDF":LogNnewRTDF,
      "FirstDateOld":LogFirstDateOld,
      "LastDateOld":LogLastDateOld,
      "FirstDateNew":LogFirstDateNew,
      "LastDateNew":LogLastDateNew}

filename = os.path.join(Root,FolderProject,"Clean.log")
AppendStringToFile(filename,log)

