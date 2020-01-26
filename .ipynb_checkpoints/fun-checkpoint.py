import json
import sys
import time
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
from tweepy import API
import os
import itertools
from ast import literal_eval
import numpy as np
from datetime import datetime
from IPython.display import clear_output, display
import json
from shutil import copyfile
import pandas as pd
import json
import pickle





def DefineAuthentication(filename):

	auth = []
	f = open(filename, "r")
	for line in f:
	    auth.append(line.strip())
	f.close()

	return auth


class TwitterListener(StreamListener):

    def on_data(self, data):
        ParsingFunction(data)
        return(True)

    def on_error(self, status):
        print(status)



def LauchScrapping(AuthFilename,TrendingTopics,LangList):

	# Authentification du compte Twitter
	auth = DefineAuthentication(AuthFilename)

	# Création du Listener
	Listener = TwitterListener()

	# Création de l'authitificateur
	auth_key = OAuthHandler(auth[0], auth[1])
	auth_key.set_access_token(auth[2], auth[3])

	# Création du LiveStream
	LiveStream = Stream(auth_key, Listener)

	# Création de l'instance API
	api= API(auth_key)

	# Lauch Scrapping
	LiveStream.filter(track=TrendingTopics, languages = LangList)

	return None


def ParsingFunction(StreamedData):

	# Load Json
	tweet = json.loads(StreamedData)
	rtbool = False

	# Storing Data
	StructuredInformations = dict(

		# User Informations
		USERNAME = str(tweet["user"]["screen_name"]),
		USERFNAME = str(tweet["user"]["name"]),
		USERID = str(tweet["user"]["id_str"]),
		USERVERIFIED = str(tweet["user"]["verified"]),
		USERDESCRIPTION = str(tweet["user"]["description"]),
		USERFOLLOWERS = str( tweet["user"]["followers_count"]),

		# Tweet Informations
		TWEETID = str(tweet["id_str"]),
		TWEETCONTENT = str(tweet["text"]),
		TWEETHASHTAGS = tweet["entities"]["hashtags"],
		TWEETTIMESTAMP = str(tweet["created_at"])
		)

	# Storing Retweet informations
	if "retweeted_status" in tweet:
		rtbool = True
		RetweetInformations = dict(

			# Author Informations
			AUTHORNAME = str(tweet["retweeted_status"]["user"]["screen_name"]),
			AUTHORFNAME = str(tweet["retweeted_status"]["user"]["name"]),
			AUTHORID = str(tweet["retweeted_status"]["user"]["id_str"]),
			AUTHORVERIFIED = str(tweet["retweeted_status"]["user"]["verified"]),
			AUTHORDESCRIPTION = str(tweet["retweeted_status"]['user']['description']),
			AUTHORFOLLOWERS = str(tweet["retweeted_status"]['user']['followers_count']),

			# Tweet Author Description
			AUTHORTWEETID = str(tweet["retweeted_status"]["id_str"]),
			AUTHORTWEETCONTENT = str(tweet["retweeted_status"]["text"]),
			AUTHORTWEETTIMESTAMP = str(tweet["retweeted_status"]["created_at"]),

			# Retweet Boolean
			ISRETWEET = rtbool

			)

	else:
		rtbool = False
		RetweetInformations = dict(
			AUTHORNAME ="",
			AUTHORFNAME ="",
			AUTHORID = "",
			AUTHORVERIFIED = "",
			AUTHORDESCRIPTION ="",
			AUTHORFOLLOWERS = "",
			AUTHORTWEETID = "",
			AUTHORTWEETCONTENT = "",
			AUTHORTWEETTIMESTAMP = "",
			ISRETWEET = rtbool
			)


	# Concatening Stuff and Printing
	FinalDic = {**StructuredInformations, **RetweetInformations}
	print(FinalDic)


	return None


def ListenningMessages(AuthFilename,TrendingTopics,LangList):
	while True:
		
		try:
			LauchScrapping(AuthFilename,TrendingTopics,LangList)
		
		except KeyboardInterrupt as e:
			break
		
		except:
			continue
	return None
























def CreateFolder(Root,FolderProject):

    AbsoluteFolderPath = os.path.join(Root,FolderProject)
    AbsConfigPath = os.path.join(AbsoluteFolderPath,"config.json")
    
    if not os.path.exists(AbsoluteFolderPath):
        os.makedirs(AbsoluteFolderPath)
    else:
        with open(AbsConfigPath, 'r') as f:
            DicConfig = json.load(f)
        print(DicConfig['TrendingTopics'])

    return None


def SaveConfig(DicConfig,Root,FolderProject):
    
    AbsoluteFolderPath = os.path.join(Root,FolderProject)
    AbsConfigPath = os.path.join(AbsoluteFolderPath,"config.json")
    
    with open(AbsConfigPath, 'w') as f:
        json.dump(DicConfig, f)
    
    return None


def WriteStreamPythonString(Root,FolderProject):
    
    cmd1 = "Root = '"+ Root+"'"
    cmd2 = "FolderProject ='" + FolderProject + "'"

    a = """
    import json
    import os
    """
    b = "\n"+cmd1+"\n"+cmd2+"\n"
    c = """
    AbsConfigPath = os.path.join(Root,FolderProject,'config.json')

    with open(AbsConfigPath, 'r') as f:
        DicConfig = json.load(f)

    for k,v in DicConfig.items():
        exec(k + '=' + 'v')

    import sys
    import time
    from tweepy import OAuthHandler, Stream
    from tweepy.streaming import StreamListener
    from tweepy import API
    sys.path.append(Root)
    from fun import *

    AuthFilename = os.path.join(Root,AuthFilename)

    ListenningMessages(AuthFilename,TrendingTopics,LangList)
    """
    PythonScript = a.replace("\n    ","\n") + b.replace("\n    ","\n") + c.replace("\n    ","\n")

    return PythonScript


def WriteStringToScript(StringScript,Path):

    with open(Path, 'w') as out:
        out.write(StringScript)

    return None


def WriteStreamBashString(PythonRoot,TweetsFilename):
    
    a = PythonRoot
    b = " RunTracking.py 1>>"
    c = TweetsFilename
    BashScript = a + b + c

    return BashScript



def CopyFilesToDirectory(OriginalFilesToCopy,SourceFolderPath,TargetFolderPath):
    for af in OriginalFilesToCopy:
        AbsSourcePath = os.path.join(SourceFolderPath,af)
        AbsDestinationPath = os.path.join(TargetFolderPath,af)
        copyfile(AbsSourcePath, AbsDestinationPath)
    return None















def LoadJsonFile(filename): 
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









def ConvertAndRunScript(JupyterRoot,PythonRoot,Root,FolderProject,NotebookName):

    a = JupyterRoot
    b = " nbconvert --to script "
    c = Root
    d = FolderProject
    e = "/"
    f = NotebookName
    g = ".ipynb"
    line1 = a+b+c+d+e+f+g

    a = PythonRoot
    b = " "
    c = Root
    d = FolderProject
    e = "/"
    f = NotebookName
    g = ".py"
    line2 = a+b+c+d+e+f+g

    script = line1 + "\n" + line2 + "\n"

    return script


def WriteStringToFile(String,Path):    
    with open(Path, 'w') as out:
        out.write(String)
    return None


def WriteCleaningScript(RowsToRemove,Root,FolderProject,TweetsFilename):
    a = "tail -n +"
    b = str(RowsToRemove)
    c = " " + os.path.join(Root,FolderProject,TweetsFilename)
    d = " > "
    e = os.path.join(Root,FolderProject,TweetsFilename)
    commandbash = a+b+c+d+e
    return commandbash