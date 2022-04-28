#The purpose of this program is to report data to the user by connecting to and querying an SQL database.

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.sql import text
from sqlalchemy import select, and_
import subprocess as sp
import pyodbc
import pandas as pd
import configparser

def getUserInputs():
    filter = input('Filter for rows where filterColumn contains:')
    selection = input('Select item to view/analyze:\n'
              'Selection 1: Enter "a"\n'
              'Selection 2: Enter "b"\n'
              'Selection 3: Enter "c"')
    return filter, selection

def engineCreation(username, password, server, database):
    dataConnect = pyodbc.connect("DRIVER={SQL Server}; SERVER=" +server+";DATABASE="+database+";UID="+username+";PWD="+ password)
    return dataConnect

def printQueryResults(dataConnect, filter, selection):
    if selection.lower() == "a":
        queryA = "SELECT a.columnHeaderFromTable2, a.sharedColumnHeader, b.columnHeaderFromTable1, b.filterColumnHeader, a.columnHeaderQueryA FROM tableName2 a JOIN tableName1 b ON a.sharedColumnHeader = b.columnHeaderFromTable1 WHERE b.filterColumnHeader = '"+filter+"'"
        df = pd.read_sql(queryA, dataConnect)
        print(df.groupby(['columnHeaderQueryA']).size())
        print(round(df['columnHeaderQueryA'].value_counts(normalize=True) * 100), 2)
    elif selection.lower() == "b":
        queryB = "SELECT a.columnHeaderFromTable3, a.sharedColumnHeader, b.columnHeaderFromTable1, b.filterColumnHeader, a.columnHeaderQueryB FROM tableName3 a JOIN tableName1 b ON a.sharedColumnHeader = b.columnHeaderFromTable1 WHERE b.filterColumnHeader = '" +filter+ "'"
        df = pd.read_sql(queryB, dataConnect)
        print(df['columnHeaderQueryB'])
    elif selection.lower() == "c":
        queryC = "SELECT a.columnHeaderFromTable4, a.sharedColumnHeader, b.columnHeaderFromTable1, b.filterColumnHeader, a.columnHeaderQueryC FROM tablename4 a JOIN tableName1 b ON a.sharedColumnHeader = b.columnHeaderFromTable1 WHERE b.filterColumnHeader = '" +filter+ "'"
        df = pd.read_sql(queryC, dataConnect)
        print(df['columnHeaderQueryC'].describe())
    else:
        print("One or more inputs is invalid")

def main():
    configFilePath = "<your config file path here>"
    config = configparser.ConfigParser()
    config.read(configFilePath)
    username = config['LOGIN']['user']
    password = config['LOGIN']['password']
    server = config['SERVER']['server']
    database = config['DATABASE']['database']
    dataConnect = engineCreation(username, password, server, database)
    filter = getUserInputs()
    selection = getUserInputs()
    print(filter + selection)
    printQueryResults(dataConnect=dataConnect, filter=filter, selection=selection)

main()