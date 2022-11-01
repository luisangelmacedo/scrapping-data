import function.XMLData as XMLData
import function.FilesFunctions as FilesFunctions
import pandas as pd
import function.ConnectDatabase as ConnectDatabase
from datetime import datetime

try:
  url = "ssh://git@globaldevtools.bbva.com:7999/btkmd/btkmd_xml_dimension_controlm.git"
  repoDir = f"control_m_{datetime.now().strftime('%Y%m%d%H%I%M')}"
  archivos = list()
  df1f = list()
  df2f = list()
  df1a = list()
  df2a = list()

  XMLData.getClonedRepository(url, repoDir)
  FilesFunctions.getFolderContent(repoDir, archivos)

  print(datetime.now(), "Filtering xml files from list")
  archivos = list(
    filter(
      lambda x: x[0] == "file", 
      archivos
    )
  )
  archivos = list(
    filter(
      lambda x: x[1].endswith("xml"), 
      archivos
    )
  )

  print(datetime.now(), "Looping list to get only job list")
  for i in archivos:
    if i != None:
      df1 = list()
      df1 = XMLData.getXMLJobs(i[1])
      df1a.append(df1)

  print(datetime.now(), "Looping list to get only variable list")
  for i in archivos:
    if i != None:
      df2 = list()
      df2 = XMLData.getXMLJobVars(i[1])
      df2a.append(df2)

  print(datetime.now(), "Filtering None values from list")
  df1a = list(
    filter(None, df1a)
  )
  df2a = list(
    filter(None, df2a)
  )

  print(datetime.now(), "Looping list for jobs")
  for i in df1a:
    for j in i:
      df1f.append(j)

  print(datetime.now(), "Looping list for variables")
  for i in df2a:
    for j in i:
      df2f.append(j)

  print(datetime.now(), "Defining job dataframe headers")
  headersJobs = [
    "job_name",
    "folder",
    "application",
    "sub_application",
    "run_as",
    "node_id",
    "max_wait",
    "cmd_line"
  ]

  print(datetime.now(), "Defining variable dataframe headers")
  headersVars = [
    "job_name",
    "param_type",
    "param_name",
    "param_value"
  ]

  print(datetime.now(), "Loading from lists to dataframes")
  jobs = pd.DataFrame(df1f, columns=headersJobs)
  vars = pd.DataFrame(df2f, columns=headersVars)

  engine = ConnectDatabase.connCreateEngine()
  conn = ConnectDatabase.connPyMySQL()

  print(datetime.now(), "Getting differences between database and parsed jobs")
  jobsReaded = pd.read_sql(
    "SELECT * FROM job",
    engine.connect()
  )
  jobs["max_wait"] = jobs["max_wait"].astype(int)

  modJobs = jobs.merge(
    jobsReaded,
    on = [
      "job_name",
      "folder",
      "application",
      "sub_application",
      "max_wait",
      "node_id",
      "run_as",
      "cmd_line"
    ], 
    indicator = "res", 
    suffixes = ("","_y"), 
    how = "outer"
  )
  modJobs = modJobs[modJobs.res == "left_only"]
  modJobs = modJobs.merge(
    jobsReaded,
    on = ["job_name"],
    suffixes = ("","_y"), 
    how = "left"
  )
  modJobs = modJobs[[
    "job_id_y",
    "job_name",
    "folder",
    "application",
    "sub_application",
    "run_as",
    "node_id",
    "max_wait",
    "cmd_line"
  ]]

  print(datetime.now(), "Inserting job(s) on database")
  if modJobs["job_name"].count() == 0:
    print(datetime.now(), "There are no new job(s) to insert")
  else:
    print(datetime.now(), f"Inserting {modJobs['job_name'].count()} job(s)")
    modJobs.to_sql(
      "job_tmp", 
      con = engine, 
      if_exists = "replace", 
      chunksize = 1000,
      index = False
    )
    print(datetime.now(), "Executing sp_job_merge()")
    conn.execute("CALL sp_job_merge()")

  print(datetime.now(), "Getting differences between database and parsed vars")
  jobsReaded = pd.read_sql(
    "SELECT * FROM job",
    engine.connect()
  )
  varsReaded = pd.read_sql(
    "SELECT * FROM job_params",
    engine.connect()
  )
  modVars = vars.merge(
    jobsReaded,
    on = ["job_name"],
    suffixes = ("","_y"),
    how = "left"
  )
  modVars = modVars.merge(
    varsReaded,
    on = [
      "job_id",
      "param_type",
      "param_name"
    ], 
    indicator = "res", 
    suffixes = ("","_y"), 
    how = "outer"
  )
  modVars = modVars[modVars.res=="left_only"]
  modVars = modVars[[
    "job_id",
    "param_type",
    "param_name",
    "param_value"
  ]]

  print(datetime.now(), "Inserting job var(s) on database")
  if modVars["job_id"].count() == 0:
    print(datetime.now(), "There are no modified var(s) to insert")
  else:
    print(datetime.now(), f"Inserting {modVars['job_id'].count()} var(s)")
    modVars.to_sql(
      "job_params_tmp", 
      con = engine, 
      if_exists = "replace", 
      chunksize = 10000,
      index = False
    )
    print(datetime.now(), "Executing sp_job_params_merge()")
    conn.execute("CALL sp_job_params_merge()")

except Exception as e:
  print(datetime.now(), "An error has ocurred, please see the folowing message")
  print(datetime.now(), e)
  print(datetime.now(), "Process finished with errors")