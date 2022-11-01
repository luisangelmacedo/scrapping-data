import function.ScrappingData as sd
import function.ConnectDatabase as ConnectDatabase
import pandas as pd
from datetime import date, timedelta, datetime
import os

try:
  print(datetime.now(), "Process started")
  dateStart = str(date.today() + timedelta(days=-1))
  dateEnd = str(date.today())
  month = date.today().strftime('%Y%m')

  print(datetime.now(), "Getting AJF jobs")
  dfAJF = sd.getAJFjobs('PE-DATIO')

  print(datetime.now(), "Getting ConsultaStatus jobs")
  dfCStat = sd.getTMPjobs('PE-DATIO', dateStart, dateEnd)

  print(datetime.now(), "Getting blacklisted jobs")
  dfBL = sd.getBlacklistJobs()

  print(datetime.now(), "Getting job_execution_ajf from database")
  engine = ConnectDatabase.connCreateEngine()
  AJFReaded = pd.read_sql(
    "SELECT * FROM job_execution_ajf", 
    engine.connect()
  )

  print(datetime.now(), "Getting jobs from database")
  jobsReaded = pd.read_sql(
    "SELECT job_id, job_name FROM job",
    engine.connect()
  )

  print(datetime.now(), "Getting job_execution_cstat from database")
  CStatReaded = pd.read_sql(
    f"""
    SELECT * FROM job_execution_cstat
    WHERE start_time BETWEEN {dateStart.replace('-','')}
    AND {dateEnd.replace('-','')}""",
    engine.connect()
  )

  print(datetime.now(), "Creating dataframe for blacklisted jobs")
  dfBL = dfBL.merge(
    jobsReaded,
    on = ["job_name"],
    how = "left"
  )

  print(datetime.now(), "Creating dataframe for AJF")
  dfAJF = dfAJF.merge(
    AJFReaded,
    on = [
      "job_name",
      "order_id",
      "order_date",
      "start_time"
    ],
    indicator = "existsDatabase",
    suffixes = ("", "_y"),
    how = "outer"
  )
  dfAJF = dfAJF[
    dfAJF.existsDatabase == "left_only"
  ]
  dfAJF = dfAJF.merge(
    jobsReaded,
    on = ["job_name"],
    suffixes = ("", "_y"),
    how = "left"
  )

  print(datetime.now(), "Creating dataframe for ConsultaStatus")
  dfCStat = dfCStat.merge(
    CStatReaded,
    on = [
      "job_name",
      "order_id",
      "order_date",
      "start_time"
    ],
    indicator = "existsDatabase",
    suffixes = ("", "_y"),
    how = "outer"
  )
  dfCStat = dfCStat[
    dfCStat.existsDatabase == "left_only"
  ]
  dfCStat = dfCStat.merge(
    jobsReaded,
    on = ["job_name"],
    suffixes = ("", "_y"),
    how = "left"
  )

  print(datetime.now(), "Homoling dataframes for AJF and Status")
  dfAJF = dfAJF[[
    'job_id_y',
    'job_name',
    'order_id',
    'folder',
    'application',
    'sub_application',
    'order_date',
    'start_time',
    'end_time',
    'host',
    'run_as',
    'execution_status'
  ]]

  dfCStat = dfCStat[[
    'job_id_y',
    'job_name',
    'order_id',
    'folder',
    'application',
    'sub_application',
    'order_date',
    'start_time',
    'end_time',
    'host',
    'run_as',
    'execution_status'
  ]]

  dfBL = dfBL[[
    "job_id",
    "job_name",
    "folder",
    "register_date",
    "comment"
  ]]

  print(datetime.now(), "Setting end and start time with YYYYMMDDHHMISS format")
  dfCStat["start_time"] = dfCStat["start_time"].str.replace(":","")
  dfCStat["start_time"] = dfCStat["start_time"].str.replace("-","")
  dfCStat["start_time"] = dfCStat["start_time"].str.replace(" ","")
  dfCStat["end_time"] = dfCStat["end_time"].str.replace(":","")
  dfCStat["end_time"] = dfCStat["end_time"].str.replace("-","")
  dfCStat["end_time"] = dfCStat["end_time"].str.replace(" ","")

  print(datetime.now(), "Adding source_origin field to both dataframes")
  dfAJF = dfAJF.assign(source_origin="AJF")
  dfCStat = dfCStat.assign(source_origin="ConsultaStatus")

  print(datetime.now(), "Filtering null values for both dataframes")
  dfAJF = dfAJF[~dfAJF.job_name.isnull()]
  dfCStat = dfCStat[~dfCStat.job_name.isnull()]

  engine = ConnectDatabase.connCreateEngine()
  conn = ConnectDatabase.connPyMySQL()

  print(datetime.now(), f"Inserting {dfBL['job_name'].count()} Blacklist row(s)")
  dfBL.to_sql(
    "job_blacklist", 
    con = engine, 
    if_exists = "replace", 
    chunksize = 10000,
    index = False
  )

  print(datetime.now(), f"Inserting {dfAJF['job_name'].count()} AJF row(s)")
  dfAJF.to_sql(
    "job_execution_ajf_tmp", 
    con = engine, 
    if_exists = "replace", 
    chunksize = 10000
  )

  print(datetime.now(), f"Inserting {dfCStat['job_name'].count()} ConsultaStatus row(s)")
  dfCStat.to_sql(
    "job_execution_cstat_tmp", 
    con = engine, 
    if_exists = "replace", 
    chunksize = 10000
  )

  print(datetime.now(), "Executing stored procedures sp_job_execution_ajf_merge")
  conn.execute("call sp_job_execution_ajf_merge()")
  print(datetime.now(), "Executing stored procedures sp_job_execution_cstat_merge")
  conn.execute("call sp_job_execution_cstat_merge()")
  print(datetime.now(), "Executing stored procedures sp_job_execution")
  conn.execute("call sp_job_execution()")
  print(datetime.now(), "Executing stored procedures sp_job_execution_report")
  conn.execute("call sp_job_execution_report()")

  print(datetime.now(), "Getting job_execution from database")
  exeReaded = pd.read_sql(
    f"SELECT * FROM job_execution WHERE SUBSTR(start_time,1,6)='{month}'",
    engine.connect()
  )
  exeReaded = exeReaded[[
    "job_name",
    "start_time"
  ]]

  print(datetime.now(), "Filtering jobs with more than one execution")
  exeGrouped = exeReaded.groupby("job_name").count()
  exeGrouped = exeGrouped[exeGrouped.start_time>1]
  exeGrouped = exeGrouped.reset_index()
  exeRepeated = exeReaded[exeReaded.job_name.isin(exeGrouped.job_name.values)]

  print(datetime.now(), "Cross-joining data to get all posibilites")
  exeCartesian = exeRepeated.merge(
    exeRepeated,
    on = "job_name",
    how = "inner",
    suffixes = ("","_2")
  )

  print(datetime.now(), "Filtering jobs with consecutive fails")
  exeCartesian["start_time"] = pd.to_datetime(
    exeCartesian["start_time"].str[:8], 
    format="%Y/%m/%d"
  )
  exeCartesian["start_time_2"] = pd.to_datetime(
    exeCartesian["start_time_2"].str[:8], 
    format="%Y/%m/%d"
  )
  exeCartesian = exeCartesian.assign(
    dif = exeCartesian["start_time"]-exeCartesian["start_time_2"]
  )
  exeCartesian = exeCartesian[
    exeCartesian.dif.dt.days == 1
  ]
  blCandidates = exeCartesian.groupby("job_name")["start_time"].max()
  blCandidates = blCandidates.reset_index()

  print(datetime.now(), f"Inserting {blCandidates['job_name'].count()} BLCandidates row(s)")
  blCandidates.to_sql(
    "blacklist_candidates", 
    con = engine, 
    if_exists = "replace", 
    chunksize = 10000,
    index = False
  )

  print(datetime.now(), "Executing stored procedures sp_blacklist_candidates")
  conn.execute("call sp_blacklist_candidates()")
  print(datetime.now(), "Process finished succesfuly")

except Exception as e:
  print(datetime.now(), "An error has ocurred, please see the folowing message")
  print(datetime.now(), e)
  print(datetime.now(), "Process finished with errors")