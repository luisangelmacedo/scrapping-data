from git import Repo
from xml.dom import minidom
from datetime import datetime

def getClonedRepository(repositoryURL, outputPath):
    try:
        print(datetime.now(), f"Getting repository {repositoryURL}")
        Repo.clone_from(repositoryURL,
                        outputPath,
                        env={"GIT_SSH_COMMAND": 'ssh -i id_ed25519'})
        print(datetime.now(), "Repository succesfuly downloaded")
    except Exception as e:
        print(datetime.now(), e)
        print(datetime.now(), "Process terminated")
        exit()

def getXMLJobs(XMLPath):
    print(datetime.now(), f"Parsing XML from {XMLPath}")
    xml = minidom.parse(XMLPath)
    xmlContent = xml.getElementsByTagName("JOB")
    jobsList = list()
    Jobname = "Undefined"
    Folder = "Undefined"
    Application = "Undefined"
    SubApplication = "Undefined"
    RunAs = "Undefined"
    NodeId = "Undefined"
    MaxWait = "Undefined"
    CmdLine = "Undefined"
    
    print(datetime.now(), "Looping parsed file to get attributes")
    for i in xmlContent:
        if i.hasAttribute("JOBNAME"):
            Jobname = i.attributes["JOBNAME"].value
        if i.hasAttribute("PARENT_FOLDER"):
            Folder = i.attributes["PARENT_FOLDER"].value
        if i.hasAttribute("APPLICATION"):
            Application = i.attributes["APPLICATION"].value
        if i.hasAttribute("SUB_APPLICATION"):
            SubApplication = i.attributes["SUB_APPLICATION"].value
        if i.hasAttribute("RUN_AS"):
            RunAs = i.attributes["RUN_AS"].value
        if i.hasAttribute("NODEID"):
            NodeId = i.attributes["NODEID"].value
        if i.hasAttribute("MAXWAIT"):
            MaxWait = i.attributes["MAXWAIT"].value
        if i.hasAttribute("CMDLINE"):
            CmdLine = i.attributes["CMDLINE"].value

        jobsList.append([Jobname,Folder,Application,
                         SubApplication,RunAs,NodeId,
                         MaxWait,CmdLine])
    return jobsList

def getXMLJobVars(XMLPath):
    print(datetime.now(), f"Parsing XML from {XMLPath}")
    xml = minidom.parse(XMLPath)
    xmlContent = xml.getElementsByTagName("JOB")
    varsList = list()
    
    print(datetime.now(), "Looping parsed file to get attributes")
    for i in xmlContent:
        Jobname = i.attributes["JOBNAME"].value
        ValueVar = "Undefined"
        NameVar = "Undefined"
        NameIn = "Undefined"
        OdateIn = "Undefined"
        AndOrIn = "Undefined"
        NameOut = "Undefined"
        OdateOut = "Undefined"
        SignOut = "Undefined"

        for j in i.getElementsByTagName("VARIABLE"):
            if j.hasAttribute("VALUE"):
                ValueVar = j.attributes["VALUE"].value
            if j.hasAttribute("NAME"):
                NameVar = j.attributes["NAME"].value
            varsList.append([
                Jobname, 
                "Variable", 
                NameVar,
                ValueVar
            ])
        for j in i.getElementsByTagName("INCOND"):
            if j.hasAttribute("NAME"):
                NameIn = j.attributes["NAME"].value
            if j.hasAttribute("ODATE"):
                OdateIn = j.attributes["ODATE"].value
            if j.hasAttribute("AND_OR"):
                AndOrIn = j.attributes["AND_OR"].value
            varsList.append([
                Jobname, 
                "Incondition",
                NameIn,
                f"{OdateIn},{AndOrIn}"
            ])
        for j in i.getElementsByTagName("OUTCOND"):
            if j.hasAttribute("NAME"):
                NameOut = j.attributes["NAME"].value
            if j.hasAttribute("ODATE"):
                OdateOut = j.attributes["ODATE"].value
            if j.hasAttribute("SIGN"):
                SignOut = j.attributes["SIGN"].value
            varsList.append([
                Jobname, 
                "Outcondition",
                NameOut,
                f"{OdateOut},{SignOut}"
            ])
    return varsList