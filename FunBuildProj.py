import sys
import os
import json
from shutil import copyfile



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


def WriteStreamPythonScript(Root,FolderProject):
    
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

    AbsPythonScriptPath = os.path.join(Root,FolderProject,"RunTracking.py")

    with open(AbsPythonScriptPath, 'w') as out:
        out.write(PythonScript + '\n')

    return None




def WriteStreamBashScript(TweetsFilename,Root,FolderProject):
    
    a = "/home/osboxes/anaconda3/envs/twitter/bin/python RunTracking.py 1>>"
    b = TweetsFilename
    BashScript = a + b
    AbsPythonScriptPath = os.path.join(Root,FolderProject,"ScriptRunScrapping.sh")
    with open(AbsPythonScriptPath, 'w') as out:
        out.write(BashScript + '\n')
    
    return None



def WriteUpdateBashScript(Root,FolderProject,CopyFiles=True):

    a = "cd "
    b = os.path.join(Root,FolderProject)
    line0 = a+b




    a = "/home/osboxes/anaconda3/bin/jupyter nbconvert --to script "
    b = Root
    c = FolderProject
    d = "/BatchRun.ipynb"
    line1 = a+b+c+d

    a = "/home/osboxes/anaconda3/envs/twitter/bin/python "
    b = Root
    c = FolderProject
    d = "/BatchRun.py"
    line2 = a+b+c+d




    a = "/home/osboxes/anaconda3/bin/jupyter nbconvert --to script "
    b = Root
    c = FolderProject
    d = "/CleanFlow.ipynb"
    line3 = a+b+c+d

    a = "/home/osboxes/anaconda3/envs/twitter/bin/python "
    b = Root
    c = FolderProject
    d = "/CleanFlow.py"
    line4 = a+b+c+d





    a = "/home/osboxes/anaconda3/bin/jupyter nbconvert --to script "
    b = Root
    c = FolderProject
    d = "/RefRun.ipynb"
    line5 = a+b+c+d

    a = "/home/osboxes/anaconda3/envs/twitter/bin/python "
    b = Root
    c = FolderProject
    d = "/RefRun.py"
    line6 = a+b+c+d




    a = "/home/osboxes/anaconda3/bin/jupyter nbconvert --to script "
    b = Root
    c = FolderProject
    d = "/RelevantTweet.ipynb"
    line7 = a+b+c+d

    a = "/home/osboxes/anaconda3/envs/twitter/bin/python "
    b = Root
    c = FolderProject
    d = "/RelevantTweet.py"
    line8 = a+b+c+d





    ScriptComputeStats = line0+ "\n"+line1 + "\n" + line2 + "\n" + line3 + "\n" + line4 + "\n" + line5 + "\n" + line6 + "\n"
    ScriptComputeStats = ScriptComputeStats + line7 + "\n" + line8 + "\n"
    AbsPythonScriptPath = os.path.join(Root,FolderProject,"ScriptUpdatingReference.sh")
    with open(AbsPythonScriptPath, 'w') as out:
        out.write(ScriptComputeStats + '\n')


    if CopyFiles:
        OriginalFilesToCopy = ["BuildGraph.ipynb","BuildNetwork.ipynb"]
        for af in OriginalFilesToCopy:
            AbsSourcePath = os.path.join(Root,af)
            AbsDestinationPath = os.path.join(Root,FolderProject,af)
            copyfile(AbsSourcePath, AbsDestinationPath)

    return None



def CopyFilesToDirectory(OriginalFilesToCopy,SourceFolderPath,TargetFolderPath):
    for af in OriginalFilesToCopy:
        AbsSourcePath = os.path.join(SourceFolderPath,af)
        AbsDestinationPath = os.path.join(TargetFolderPath,af)
        copyfile(AbsSourcePath, AbsDestinationPath)
    return None