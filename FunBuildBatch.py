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


def LoadJsonFile(filename,NameVariable): 
    with open(filename, 'r') as f:
        DicConfig = json.load(f)
    return DicConfig


def GlobalDicDeplier(OneDic):
    for k,v in OneDic.items():
        exec('globals()[k] = v')
    return None


def ReadTweetsToList(Root,FolderProject,TweetsFilename,sizetweet=20):

    errors = 0
    filename = os.path.join(Root,FolderProject,TweetsFilename)
    tweetslist = []

    with open(filename) as fp:
        for line in fp: 
            try:
                jsontweet = literal_eval(line)
                if type(jsontweet) is dict and len(jsontweet)==sizetweet:
                    tweetslist.append(jsontweet)
                else:
                    errors = errors +1
            except:
                errors = errors + 1

    return tweetslist,errors 



def PickleDump(filename,objitem):

    outfile = open(filename,'wb')
    pickle.dump(objitem,outfile)
    outfile.close()

    return None


