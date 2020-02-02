#!/usr/bin/env python
# coding: utf-8

# In[91]:


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


# In[92]:


def LoadJsonFile(filename): 
    with open(filename, 'r') as f:
        DicConfig = json.load(f)
    return DicConfig


def GlobalDicDeplier(OneDic):
    for k,v in OneDic.items():
        exec('globals()[k] = v')
    return None


# In[93]:


DicConfig = LoadJsonFile(os.path.join(os.getcwd(),"config.json"))
GlobalDicDeplier(DicConfig)
sys.path.append(Root)
from fun import *

print("Load Config variables")


# In[ ]:


print("Load Data")


# In[94]:


path = os.path.join(Root,FolderProject,"ISdf.pkl")
ISdf = LoadPickleOrInit(path)

path = os.path.join(Root,FolderProject,"RefFam.pkl")
RefFam = LoadPickleOrInit(path)

path = os.path.join(Root,FolderProject,"RefRT.pkl")
RefRT = LoadPickleOrInit(path)


# In[95]:


if len(ISdf)>0:
    
    RefFamMemory = RetrieveSize(os.path.join(Root,FolderProject,"RefFam.pkl"))
    RefRTMemory = RetrieveSize(os.path.join(Root,FolderProject,"RefRT.pkl"))
    RefFamRows = len(RefFam)
    RefRTRows = len(RefRT)

    minbassine = ISdf.BassineID.min()
    maxbassine = ISdf.BassineID.max()
    print("Define Metaparameters")


# # Build KeptTweets

# In[ ]:


if len(ISdf)>0:
    print("Building Keep tweets")
    BassineID = RefFam.AUTHORTWEETUNIXEPOCH / bassine_size
    RefFam["BassineID"] = BassineID.astype(int)

    RefFamKO = RefFam.copy()[RefFam.status=="ko"]
    RefFamOK = RefFam.copy()[RefFam.status=="ok"]
    RefRTKO = RefRT.copy()[RefRT.status=="ko"]
    RefRTOK = RefRT.copy()[RefRT.status=="ok"]
    RefRTKO.drop(columns="status",inplace=True)

    RefFamKORows = len(RefFamKO)
    RefFamOKRows = len(RefFamOK)
    RefRTKORows = len(RefRTKO)
    RefRTOKRows = len(RefRTOK)


# # RefFam

# In[100]:


if len(ISdf)>0:
    print("Building RefFam")

    RefFamKOa = RefFamKO.copy().merge(ISdf["AUTHORTWEETID"],on="AUTHORTWEETID")
    RefFamKOb = RefFamKO.copy()[RefFamKO.BassineID>maxbassine]
    RefFamKOa["status"] = "ok"
    RefFamKOb["status"] = "ko"

    RefFamKOaRows = len(RefFamKOa)
    RefFamKObRows = len(RefFamKOb)
    RefFamKOFinal = pd.concat([RefFamKOa,RefFamKOb],axis = 0, sort = True)
    RefFamKOFinal.reset_index(drop=True,inplace=True)
    RefFamTemp = pd.concat([RefFamKOFinal,RefFamOK],axis=0,sort=True)
    RefFamKOFinalRows = len(RefFamKOFinal)
    RefFamTempRows = len(RefFamTemp)
    RefRTKO = RefRTKO.merge(RefFamKOFinal[["AUTHORTWEETID","status"]],on="AUTHORTWEETID")
    RefRTTemp = pd.concat([RefRTKO,RefRTOK],axis=0,sort=True)
    RefRTTemp.reset_index(drop=True,inplace=True)
    RefRTKORows = len(RefRTKO)
    RefRTTempRows = len(RefRTTemp)
    RefFamOKKODis = RefFamTemp.status.value_counts().tolist()
    RefRTTOKKODis = RefRTTemp.status.value_counts().tolist()


# # Sauvegarde

# In[107]:


if len(ISdf)>0:
    print("Sauvegarde")
    PickleDump(os.path.join(Root,FolderProject,"RefFam.pkl"),RefFamTemp)
    PickleDump(os.path.join(Root,FolderProject,"RefRT.pkl"),RefRTTemp)


# # Logs

# In[108]:


if len(ISdf)>0:
    RefRTTempMemory = RetrieveSize(os.path.join(Root,FolderProject,"RefRT.pkl"))
    RefFamTempMemory = RetrieveSize(os.path.join(Root,FolderProject,"RefFam.pkl"))

    UpLogs = {"DateRun" : GetCurrentTime(),
    "RefFamMemory" : RefFamMemory,
    "RefRTMemory" : RefRTMemory,
    "RefFamRows" : FormatNumber(RefFamRows),
    "RefRTRows"  : FormatNumber(RefRTRows),
    "minbassine" : minbassine,
    "maxbassine" : maxbassine,
    "RefFamKORows"  : FormatNumber(RefFamKORows),
    "RefFamOKRows"  : FormatNumber(RefFamOKRows),
    "RefRTKORows"  : FormatNumber(RefRTKORows),
    "RefRTOKRows"  : FormatNumber(RefRTOKRows),
    "RefFamKOaRows"  : FormatNumber(RefFamKOaRows),
    "RefFamKObRows"  : FormatNumber(RefFamKObRows),
    "RefFamKOFinalRows" : FormatNumber(RefFamKOFinalRows),
    "RefFamTempRows"  : FormatNumber(RefFamTempRows),
    "RefRTKORows"  : FormatNumber(RefRTKORows),
    "RefRTTempRows"  : FormatNumber(RefRTTempRows),
    "RefFamOKKODis" : RefFamOKKODis,
    "RefRTTOKKODis" : RefRTTOKKODis,
    "RefRTTempMemory": RefRTTempMemory,
    "RefFamTempMemory":RefFamTempMemory}


    filename = os.path.join(Root,FolderProject,"Up.log")
    AppendStringToFile(filename,UpLogs)
    print("Write logs")


# In[109]:





# In[ ]:




