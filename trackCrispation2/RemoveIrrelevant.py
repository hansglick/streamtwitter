#!/usr/bin/env python
# coding: utf-8

# In[51]:


import pandas as pd
import json
import pickle
import os
import itertools
from ast import literal_eval
import numpy as np
import sys
from datetime import datetime
from IPython.display import clear_output, display
import subprocess
from datetime import datetime
pd.options.display.float_format = '{:.0f}'.format


# In[52]:


def LoadJsonFile(filename): 
    with open(filename, 'r') as f:
        DicConfig = json.load(f)
    return DicConfig


def GlobalDicDeplier(OneDic):
    for k,v in OneDic.items():
        exec('globals()[k] = v')
    return None


# In[53]:


DicConfig = LoadJsonFile(os.path.join(os.getcwd(),"config.json"))
GlobalDicDeplier(DicConfig)
sys.path.append(Root)
from fun import *

print("Load Config variables")


# In[54]:


path = os.path.join(Root,FolderProject,"RefFam.pkl")
RefFam = LoadPickleOrInit(path)

path = os.path.join(Root,FolderProject,"RefRT.pkl")
RefRT = LoadPickleOrInit(path)

path = os.path.join(Root,FolderProject,"ISdf.pkl")
OldISdf = LoadPickleOrInit(path)

print("Load Data")


# In[55]:


FirstBassineID = ExtractFirstBID(RefRT,OldISdf,bassine_size)
datelastaction = RefRT.TWEETUNIXEPOCH.max()
print("Define Metaparameters")


# In[56]:


def ExtractQualifiedRT(RefFam,bassine_size,FirstBassineID):

    df = RefRT.copy()

    BassineID = RefFam.AUTHORTWEETUNIXEPOCH/bassine_size
    RefFam["BassineID"] = BassineID.astype(int)
    RefFam["BassineSize"] = bassine_size
    RefFam = RefFam[RefFam.BassineID>=FirstBassineID]
    RefFam.reset_index(drop=False,inplace=True)

    df = df[df.AUTHORTWEETID.isin(RefFam.AUTHORTWEETID)]
    df.reset_index(drop=True,inplace=True)
    df = df.merge(RefFam[["AUTHORTWEETID","AUTHORTWEETUNIXEPOCH"]],on="AUTHORTWEETID")

    return df


# In[57]:


def AddSomeStatsRT(df,bassine_size,bassine_recul,FirstBassineID):

    BassineID = df.AUTHORTWEETUNIXEPOCH/bassine_size
    df["BassineID"] = BassineID.astype(int)
    df["BassineSize"] = bassine_size
    df["age"] = df.TWEETUNIXEPOCH - df.AUTHORTWEETUNIXEPOCH
    df["recul"] = datelastaction - df.AUTHORTWEETUNIXEPOCH

    minreculdf = df.groupby("BassineID")["recul"].min().reset_index()
    minreculdf = minreculdf[minreculdf.recul>=bassine_recul]
    minreculdf = minreculdf[minreculdf.BassineID>=FirstBassineID]
    minreculdf = minreculdf.rename(columns = {"recul":"minrecul"}).reset_index(drop = True)
    
    df = df.merge(minreculdf,on="BassineID")
    df = df[df.age<=bassine_recul]
    
    return df


# In[58]:


def BuildStatsIrrelevant(tempdf,bassine_threshold,bassine_size):

    y = tempdf.groupby(["BassineID","AUTHORTWEETID"]).size().reset_index().rename(columns = {0:"f"})
    r = y.groupby('BassineID')['f'].rank(ascending=False).reset_index(drop=True)
    y["Rank"] = r
    y.sort_values(by=["BassineID","f"],ascending=False,inplace=True)
    y.reset_index(drop = True,inplace = True)
    y["cumsumf"] = y.groupby("BassineID")["f"].apply(lambda x: x.cumsum())
    sumdf = y.groupby("BassineID")["f"].sum().reset_index().rename(columns = {"f":"sum"})
    y = y.merge(sumdf,on="BassineID")
    y["p"] = (100*y.cumsumf) / y["sum"]
    y = y[y.p<bassine_threshold]
    y["BassineSize"]  = bassine_size

    return y


# In[59]:


def UpdateISdf(RefFam,bassine_size,FirstBassineID,bassine_recul,bassine_threshold,Root,FolderProject):
    
    global NQualifiedRT
    global NAddSomeStatsRT
    global NISdf
    global NISdfFinal
    
    NQualifiedRT=0
    NAddSomeStatsRT=0
    NISdf=0
    NISdfFinal=0
    
    print("Extracting Qualified RT")
    df = ExtractQualifiedRT(RefFam,bassine_size,FirstBassineID)
    NQualifiedRT = len(df)
    print("rows : ",str(NQualifiedRT))
    
    if len(df)>0:
        print("Add Some Stats to Qualified RT")
        df = AddSomeStatsRT(df,bassine_size,bassine_recul,FirstBassineID)
        NAddSomeStatsRT = len(df)
        print("rows : ",str(NAddSomeStatsRT))
        
    else:
        print("Not enough data")
        NAddSomeStatsRT = 0
        return None
    
    if len(df)>0:
        print("Compute Stats for Relevant RT")
        ISdf = BuildStatsIrrelevant(df,bassine_threshold,bassine_size)
        NISdf = len(ISdf)
        print("Stack new data to OldISdf")
        ISdf = OldISdf.append(ISdf,ignore_index=True)
        ISdf.reset_index(drop=True,inplace=True)
        NISdfFinal = len(ISdf)
        print("Save ISdf.pkl on disk")
        PickleDump(os.path.join(Root,FolderProject,"ISdf.pkl"),ISdf)
        print(str(NISdf)," ont été rajoutées")
        print(str(NISdfFinal)," au total dans le fichier ISdf.pkl")
        
    else:
        print("Not enough data")
        NISdf = 0
        NISdfFinal = len(OldISdf)
        return None
    
    return None


# In[ ]:


print("Update ISdf")


# In[60]:


UpdateISdf(RefFam,
           bassine_size,
           FirstBassineID,
           bassine_recul,
           bassine_threshold,
           Root,
           FolderProject)


# # Logs

# In[61]:


RelevantLogs = {"DateRun" : GetCurrentTime(),
         "FirstBassineID" : FirstBassineID,
         "DateLastAction" : str(pd.to_datetime(datelastaction,unit="s")),
         "NQualifiedRTA" :NQualifiedRT, 
         "NQualifiedRTB" :NAddSomeStatsRT,
         "NRelevantTweets" : NISdf,
         "TotalNRelevantTweets" : NISdfFinal}


# In[62]:


filename = os.path.join(Root,FolderProject,"Relevant.log")
AppendStringToFile(filename,RelevantLogs)

print("Write logs")


# In[ ]:





# In[ ]:





# print("Extracting Qualified RT")
# df = ExtractQualifiedRT(RefFam,bassine_size,FirstBassineID)
# print("rows : ",str(len(df)))
# 
# print("Add Some Stats to Qualified RT")
# df = AddSomeStatsRT(df,bassine_size,bassine_recul,FirstBassineID)
# print("rows : ",str(len(df)))
# 
# print("Compute Stats for Relevant RT")
# ISdf = BuildStatsIrrelevant(df,bassine_threshold,bassine_size)
# addrow = len(ISdf)
# print("Stack new data to OldISdf")
# 
# ISdf = OldISdf.append(ISdf,ignore_index=True)
# ISdf.reset_index(drop=True,inplace=True)
# finalrow = len(ISdf)
# print("Save ISdf.pkl on disk")
# 
# PickleDump(os.path.join(Root,FolderProject,"ISdf.pkl"),ISdf)
# print(str(addrow)," ont été rajoutées")
# print(str(finalrow)," au total dans le fichier ISdf.pkl")
