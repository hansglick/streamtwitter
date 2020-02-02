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


# LOAD DATA
print("Load data")
path = os.path.join(Root,FolderProject,"RefFam.pkl")
RefFam = LoadPickleOrInit(path)

path = os.path.join(Root,FolderProject,"RefRT.pkl")
RefRT = LoadPickleOrInit(path)

tweetsdf = RefFam.copy()[RefFam.status=="ok"]
rtdf = RefRT.copy()[RefRT.status=="ok"]

datefirst = rtdf.TWEETUNIXEPOCH.min()
datelast = rtdf.TWEETUNIXEPOCH.max()

OldListOfTMResults = LoadPickleOrInit(os.path.join(Root,FolderProject,"SequenceOfGraphResults.pkl"),
                                      typeobj="dic")


# In[5]:


# DEFINE PREREQUISITE VARIABLES
print("Define parameters")
firstdate,lastdate = DefineFirstLastDate(rtdf,StepSize,OldListOfTMResults)
ListOfTM = DefineTimeMarks(lastdate,firstdate,StepSize,WindowSize)


# In[6]:


# ANALYTICS
print("Analytics ...")
ListOfTMResults = ListOfTMTreatment(ListOfTM,WindowSize,StepSize,rtdf)
LogTMAddSize = len(ListOfTMResults)
print("Run Analytics : ",str(LogTMAddSize>1))


# In[7]:


print("Logs A")
if LogTMAddSize>1:

    LogBasicStats = [(str(pd.to_datetime(k,unit="s")),
                      len(v["Links"]),
                      len(v["Tweets"])) for k,v in ListOfTMResults.items()]
    LogBasicStats = np.array([item[2] for item in LogBasicStats])
    LogBasicStats = stats.describe(LogBasicStats)
    LogBasicStatsTweets = dict(LogBasicStats._asdict())

    LogBasicStats = [(str(pd.to_datetime(k,unit="s")),
                      len(v["Links"]),
                      len(v["Tweets"])) for k,v in ListOfTMResults.items()]
    LogBasicStats = np.array([item[1] for item in LogBasicStats])
    LogBasicStats = stats.describe(LogBasicStats)
    LogBasicStatsLinks = dict(LogBasicStats._asdict())


# In[8]:


print("Logs B")
if LogTMAddSize>1:
    
    ListOfTMResults = {**ListOfTMResults, **OldListOfTMResults}

    LogTMNowSize = len(ListOfTMResults)

    LogBasicStats = [(str(pd.to_datetime(k,unit="s")),
                      len(v["Links"]),
                      len(v["Tweets"])) for k,v in ListOfTMResults.items()]
    LogBasicStats = np.array([item[2] for item in LogBasicStats])
    LogBasicStats = stats.describe(LogBasicStats)
    LogBasicStatsTweets2 = dict(LogBasicStats._asdict())

    LogBasicStats = [(str(pd.to_datetime(k,unit="s")),
                      len(v["Links"]),
                      len(v["Tweets"])) for k,v in ListOfTMResults.items()]
    LogBasicStats = np.array([item[1] for item in LogBasicStats])
    LogBasicStats = stats.describe(LogBasicStats)
    LogBasicStatsLinks2 = dict(LogBasicStats._asdict())


# In[9]:


print("Writing Logs")
if LogTMAddSize>1:
# SAVE LOGS
    mylog = {"TMAddSize":LogTMAddSize,
             "TMNowSize":LogTMNowSize,
             "AddBasicStatsLinks":LogBasicStatsLinks,
             "AddBasicStatsTweets":LogBasicStatsTweets,
             "NewBasicStatsLinks":LogBasicStatsLinks2,
             "NewBasicStatsTweets":LogBasicStatsTweets2}
    filename = os.path.join(Root,FolderProject,"Graph.log")
    AppendStringToFile(filename,mylog)


# In[10]:


print("Saving Results")
if LogTMAddSize>1:
# SAVE RESULTS
    PickleDump(os.path.join(Root,FolderProject,"SequenceOfGraphResults.pkl"),ListOfTMResults)


# In[ ]:





# In[ ]:




