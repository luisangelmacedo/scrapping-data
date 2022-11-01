#imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.structures import CaseInsensitiveDict
from datetime import datetime

def getBlacklistJobs():
    url = "http://150.100.216.64:8080/scheduling/listaNegra"
    print(datetime.now(), f"Sending request to {url}")
    response = requests.get(url)
    
    #parsing
    print(datetime.now(), "Looking for tables")
    html = BeautifulSoup(response.text, 'html.parser')
    table = html.find('table', class_="tablesorter")
    rows = table.find_all('tr')
    
    #creating empty list to recieve data
    print(datetime.now(), "Defining headers for dataframe")
    data = list()
    headers = [
        "no",
        "job_name",
        "folder",
        "register_date",
        "comment"
    ]
    
    #looping rows to collect data into list
    print(datetime.now(), "Looping data to get cells")
    for i in rows:
        table_data = i.find_all('td')
        data.append([j.text for j in table_data])
    
    #filtering none values
    data = list(
        filter(None, data)
    )
    
    #loading dataframe
    df = pd.DataFrame(data, columns = headers)
    return df

def getAJFjobs(filter):
    url = "http://150.100.216.64:8080/scheduling/ajF?filtro=" + filter
    print(datetime.now(), f"Sending request to {url}{filter}")
    response = requests.get(url)
    
    #parsing
    print(datetime.now(), "Looking for tables")
    html = BeautifulSoup(response.text, 'html.parser')
    table = html.find('table', class_="tablesorter")
    rows = table.find_all('tr')
    
    #creating empty list to recieve data
    print(datetime.now(), "Defining headers for dataframe")
    data = list()
    headers = ['order_id','job_name','folder',
               'application','sub_application',
               'order_date','start_time','end_time',
               'host','run_as','execution_status']
    
    #looping rows to collect data into list
    print(datetime.now(), "Looping data to get cells")
    for i in rows:
        table_data = i.find_all('td')
        data.append([j.text for j in table_data])
    
    #loading dataframe
    df = pd.DataFrame(data, columns = headers)
    df['order_date'] = '20'+df['order_date']
    
    #return
    return df

def getTMPjobs(filter, dateStart, dateEnd):
    url = "http://150.100.216.64:8080/scheduling/ejecucionesStatusConsulta"
    
    headers = CaseInsensitiveDict()
    headers["Accept"] = "text/html, */*; q=0.01"
    headers["Accept-Language"] = "es-ES,es;q=0.9,en;q=0.8"
    headers["Connection"] = "keep-alive"
    headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    #headers["Cookie"] = "JSESSIONID=46A188447B5401127B7066FA8427A15E"
    headers["Origin"] = "http://150.100.216.64:8080"
    headers["Referer"] = "http://150.100.216.64:8080/scheduling/ejecucionesStatus"
    headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
    headers["X-Requested-With"] = "XMLHttpRequest"
    
    data = f"jobname=%25%25&txtFromDate={dateStart}&txtFromTime=00%3A00&txtToDate={dateEnd}&txtToTime=23%3A59&nodeid=&foldername=&aplicacion=%25{filter}%25"
    print(datetime.now(), f"Sending request to {url}")
    resp = requests.post(url, headers=headers, data=data)
    
    print(datetime.now(), "Looking for table on parsing")
    html = BeautifulSoup(resp.text, 'html.parser')
    table = html.find('table', class_="tablesorter")
    rows = table.find_all('tr')
    
    print(datetime.now(), "Defining headers for dataframe")
    dataTMP = list()
    headers = ['Number', 'job_name', 'folder',
               'application', 'sub_application',
               'run_as', 'order_id', 'order_date',
               'start_time', 'end_time', 'RunTime',
               'RunCounter', 'execution_status', 'host', 'CPUTime']
    
    print(datetime.now(), "Looping data to get cells")
    for i in rows:
        tableData = i.find_all('td')
        dataTMP.append([j.text for j in tableData])
    
    print(datetime.now(), "Assigning headers to dataframe")
    df = pd.DataFrame(dataTMP, columns = headers)
    print(datetime.now(), "Deleting unnecessary columns")
    df = df.drop(['Number'], axis = 1)
    df = df.drop(['RunCounter'], axis = 1)
    df = df.drop(['RunTime'], axis = 1)
    df = df.drop(['CPUTime'], axis = 1)
    return df
