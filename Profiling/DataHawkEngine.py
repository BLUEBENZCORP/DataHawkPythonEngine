from configparser import ConfigParser
import pandas as pd
import sqlalchemy as sal
from sqlalchemy import create_engine
import urllib
from urllib import parse
import pyodbc
from urllib.parse import quote
import mysql.connector
from Profiling import profiling
pd.options.display.float_format = "{:,.2f}".format


def get_table_name():
    global ConnString, DBUser, DBPassword, DBSchema, DbType
    mydb = mysql.connector.connect(
        host="43.231.124.151",
        user="root",
        password="root@123",
        database="datahawk",
        auth_plugin="caching_sha2_password"
    )
    curr = mydb.cursor()
    query = '''
            SELECT ConnString, DBUser, DBPassword, DBSchema, DbType FROM repository where ID = 133;
            '''
    curr.execute(query)
    data = curr.fetchall()
    # print(data)
    for i in data:
        ConnString = i[0]
        ConnString = ConnString.split('/')[2]
        ConnString = ConnString.split(':')[0]
        DBUser = i[1]
        DBPassword = i[2]
        DBSchema = i[3]
        DbType = i[4]
        # print("ConnString:", ConnString)
        # print("DBUser:", DBUser)
        # print("DBPassword:", DBPassword)
        # print("DBSchema:", DBSchema)
        # print("DbType:", DbType)

    # data = pd.DataFrame()
    if DbType == "MSSQL":
        param2 = urllib.parse.quote_plus("DRIVER={SQL Server};"
                                         "SERVER=" + ConnString + ";"
                                                                  "DATABASE=" + DBSchema + ";"
                                                                                           "UID=" + DBUser + ";"
                                                                                                             "PWD=" + DBPassword + ";")

        engine2 = create_engine("mssql+pyodbc:///?odbc_connect={}".format(param2))
        query = '''
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='fedemo' AND TABLE_SCHEMA='dbo' 
                '''
        data1 = pd.read_sql_query(query, engine2)
        print(data1)
        for table in data1.index:
            tableName = data1['TABLE_NAME'][table]
            query1 = "SELECT top(10000) * FROM {}".format(tableName)
            tabledata = pd.read_sql_query(query1, engine2)
            print(tabledata.info())
            repoId= 133
            repo_scan_Id = 1
            profile_data = profiling(tabledata, tableName, repoId, repo_scan_Id)
            # print(profile_data.columns)

            # print(profile_data)
            param = urllib.parse.quote_plus("DRIVER={MySQL ODBC 8.0 ANSI Driver};"
                                             "SERVER=43.231.124.151;"
                                             "DATABASE=datahawk;"
                                             "UID=root;"
                                             "PWD=root@123;")
            engine = sal.create_engine("mysql+pyodbc:///?odbc_connect={}".format(param))
            try:
                profile_data.to_sql('profile_result', engine, if_exists='append', index=False, chunksize=25000)

            except Exception as e:
                print(str(e))
                break


if __name__ == "__main__":
    get_table_name()
