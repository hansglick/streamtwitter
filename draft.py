def WriteUpdateBashString(Root,FolderProject,JupyterRoot):

    a = "cd "
    b = os.path.join(Root,FolderProject)
    line0 = a+b

    a0 = JupyterRoot
    a = " nbconvert --to script "
    b = Root
    c = FolderProject
    d = "/BatchRun.ipynb"
    line1 = a0+a+b+c+d

    a0 = PythonRoot
    a = " "
    b = Root
    c = FolderProject
    d = "/BatchRun.py"
    line2 = a0+a+b+c+d

    a0 = JupyterRoot
    a = " nbconvert --to script "
    b = Root
    c = FolderProject
    d = "/CleanFlow.ipynb"
    line3 = a0+a+b+c+d

    a0 = PythonRoot
    a = " "
    b = Root
    c = FolderProject
    d = "/CleanFlow.py"
    line4 = a0+a+b+c+d

    a0 = JupyterRoot
    a = " nbconvert --to script "
    b = Root
    c = FolderProject
    d = "/RefRun.ipynb"
    line5 = a0+a+b+c+d

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


    return ScriptComputeStats


AbsPythonScriptPath = os.path.join(Root,FolderProject,"ScriptUpdatingReference.sh")
with open(AbsPythonScriptPath, 'w') as out:
    out.write(ScriptComputeStats + '\n')


if CopyFiles:
    OriginalFilesToCopy = ["BuildGraph.ipynb","BuildNetwork.ipynb"]
    for af in OriginalFilesToCopy:
        AbsSourcePath = os.path.join(Root,af)
        AbsDestinationPath = os.path.join(Root,FolderProject,af)
        copyfile(AbsSourcePath, AbsDestinationPath)