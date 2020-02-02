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
ISdf = PickleLoad(path)

path = os.path.join(Root,FolderProject,"RefFam.pkl")
RefFam = LoadPickleOrInit(path)

path = os.path.join(Root,FolderProject,"RefRT.pkl")
RefRT = LoadPickleOrInit(path)


# In[95]:


RefFamMemory = RetrieveSize(os.path.join(Root,FolderProject,"RefFam.pkl"))
RefRTMemory = RetrieveSize(os.path.join(Root,FolderProject,"RefRT.pkl"))
RefFamRows = len(RefFam)
RefRTRows = len(RefRT)


# In[96]:


minbassine = ISdf.BassineID.min()
maxbassine = ISdf.BassineID.max()
print("Define Metaparameters")


# # Build KeptTweets

# In[ ]:


print("Building Keep tweets")


# In[97]:


BassineID = RefFam.AUTHORTWEETUNIXEPOCH / bassine_size
RefFam["BassineID"] = BassineID.astype(int)


# In[98]:


RefFamKO = RefFam.copy()[RefFam.status=="ko"]
RefFamOK = RefFam.copy()[RefFam.status=="ok"]
RefRTKO = RefRT.copy()[RefRT.status=="ko"]
RefRTOK = RefRT.copy()[RefRT.status=="ok"]
RefRTKO.drop(columns="status",inplace=True)


# In[99]:


RefFamKORows = len(RefFamKO)
RefFamOKRows = len(RefFamOK)
RefRTKORows = len(RefRTKO)
RefRTOKRows = len(RefRTOK)


# # RefFam

# In[ ]:


print("Building RefFam")


# In[100]:


RefFamKOa = RefFamKO.copy().merge(ISdf["AUTHORTWEETID"],on="AUTHORTWEETID")
RefFamKOb = RefFamKO.copy()[RefFamKO.BassineID>maxbassine]
RefFamKOa["status"] = "ok"
RefFamKOb["status"] = "ko"


# In[101]:


RefFamKOaRows = len(RefFamKOa)
RefFamKObRows = len(RefFamKOb)


# In[102]:


RefFamKOFinal = pd.concat([RefFamKOa,RefFamKOb],axis = 0, sort = True)
RefFamKOFinal.reset_index(drop=True,inplace=True)
RefFamTemp = pd.concat([RefFamKOFinal,RefFamOK],axis=0,sort=True)


# In[103]:


RefFamKOFinalRows = len(RefFamKOFinal)
RefFamTempRows = len(RefFamTemp)


# In[104]:


RefRTKO = RefRTKO.merge(RefFamKOFinal[["AUTHORTWEETID","status"]],on="AUTHORTWEETID")
RefRTTemp = pd.concat([RefRTKO,RefRTOK],axis=0,sort=True)
RefRTTemp.reset_index(drop=True,inplace=True)


# In[105]:


RefRTKORows = len(RefRTKO)
RefRTTempRows = len(RefRTTemp)


# In[106]:


RefFamOKKODis = RefFamTemp.status.value_counts().tolist()
RefRTTOKKODis = RefRTTemp.status.value_counts().tolist()


# # Sauvegarde

# In[107]:


print("Sauvegarde")
PickleDump(os.path.join(Root,FolderProject,"RefFam.pkl"),RefFamTemp)
PickleDump(os.path.join(Root,FolderProject,"RefRT.pkl"),RefRTTemp)


# # Logs

# In[108]:


RefRTTempMemory = RetrieveSize(os.path.join(Root,FolderProject,"RefRT.pkl"))
RefFamTempMemory = RetrieveSize(os.path.join(Root,FolderProject,"RefFam.pkl"))


# In[109]:


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


# In[110]:


filename = os.path.join(Root,FolderProject,"Up.log")
AppendStringToFile(filename,UpLogs)

print("Write logs")


# In[ ]:




