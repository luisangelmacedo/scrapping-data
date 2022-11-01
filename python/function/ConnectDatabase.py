#imports
import pymysql
from sqlalchemy import create_engine
from pymysql.constants import CLIENT
from datetime import datetime

#database variables
database = "***********"
region = "***********"
instance_name = "***********"
project_id = "***********"
user = "***********"
pwd = "***********"
host = "***********"
unix_socket = f'/cloudsql/{project_id}:{region}:{instance_name}'
conn_dict = {"host": host, "database": database, "user": user, 
             "password": pwd, 'charset':'utf8',
             "client_flag": CLIENT.MULTI_STATEMENTS}

#function to connect mysql with pymysql
def connPyMySQL():
  conn = None
  try:
    print(datetime.now(), "Connecting to MySQL with pymysql")
    conn = pymysql.connect(**conn_dict)
    print(datetime.now(), "Connection succesful")
    return conn.cursor()
  except (Exception, pymysql.DatabaseError) as error:
    print(datetime.now(), error)
    return False
  

#function to conect mysql with create_engine
def connCreateEngine():
  conn = None
  try:
    print(datetime.now(), "Connecting to MySQL with createEngine")
    conn = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}/{database}")
    print(datetime.now(), "Connection succesful")
  except Exception as error:
    print(datetime.now(), error)
  return conn
  