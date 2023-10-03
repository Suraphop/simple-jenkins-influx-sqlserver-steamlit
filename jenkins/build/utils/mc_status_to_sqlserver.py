import utils.constant as constant
import pandas as pd
import os
import sys
import utils.alert as alert
import pymssql
import json

from datetime import datetime,date, timedelta
from sqlalchemy import create_engine,text,engine
from influxdb import InfluxDBClient
from datetime import datetime

class PREPARE:


    def __init__(self,path,server,database,user_login,password,table,table_log,line_notify_token,initial_db,line_notify_flag):
        self.path = path
        self.server = server
        self.database = database
        self.user_login = user_login
        self.password = password
        self.table_log = table_log
        self.table = table
        self.df = None
        self.df_insert = None
        self.line_notify_token = line_notify_token
        self.initial_db = initial_db
        self.line_notify_flag = line_notify_flag
        self.path_list = None
        self.path_now = None

    def stamp_time(self):
        now = datetime.now()
        print("\nHi this is job run at -- %s"%(now.strftime("%Y-%m-%d %H:%M:%S")))

    def error_msg(self,process,msg,e):
        result = {"status":constant.STATUS_ERROR,"process":process,"message":msg,"error":e}

        try:
            print("Error: "+self.alert_error_msg(result))
            if self.line_notify_flag == "True":
                self.alert_line(self.alert_error_msg(result))
            self.log_to_db(result)
            sys.exit()
        except Exception as e:
            self.info_msg(self.error_msg.__name__,e)
            sys.exit()
    
    def alert_line(self,msg):
        value = alert.line_notify(self.line_notify_token,msg)
        value = json.loads(value)  
        if value["message"] == constant.STATUS_OK:
            self.info_msg(self.alert_line.__name__,'sucessful send to line notify')
        else:
            self.info_msg(self.alert_line.__name__,value)

    def alert_error_msg(self,result):
        if self.line_notify_token != None:
            return f'\nproject: {self.table}\nprocess: {result["process"]}\nmessage: {result["message"]}\nerror: {result["error"]}\n'
                
    def info_msg(self,process,msg):
        result = {"status":constant.STATUS_INFO,"process":process,"message":msg,"error":"-"}
        print(result)

    def ok_msg(self,process):
        result = {"status":constant.STATUS_OK,"process":process,"message":"program running done","error":"-"}
        try:
            self.log_to_db(result)
            print(result)
        except Exception as e:
            self.error_msg(self.ok_msg.__name__,'cannot ok msg to log',e)
    
    def conn_sql(self):
        #connect to db
        try:
            cnxn = pymssql.connect(self.server, self.user_login, self.password, self.database)
            cursor = cnxn.cursor()
            return cnxn,cursor
        except Exception as e:
            self.alert_line("Danger! cannot connect sql server")
            self.info_msg(self.conn_sql.__name__,e)
            sys.exit()

    def log_to_db(self,result):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            cursor.execute(f"""
                INSERT INTO [{self.database}].[dbo].[{self.table_log}] 
                values(
                    getdate(), 
                    '{result["status"]}', 
                    '{result["process"]}', 
                    '{result["message"]}', 
                    '{str(result["error"]).replace("'",'"')}'
                    )
                    """
                )
            cnxn.commit()
            cursor.close()
        except Exception as e:
            self.alert_line("Danger! cannot insert log table")
            self.info_msg(self.log_to_db.__name__,e)
            sys.exit()


class MCSTATUS(PREPARE):

    
    def __init__(self,path,server,database,user_login,password,table,table_log,initial_db,line_notify_flag,line_notify_token=None):
        super().__init__(path,server,database,user_login,password,table,table_log,line_notify_token,initial_db,line_notify_flag)        

    def read_path(self):
        path_list = []
        file_extension = '.txt'
        for root,dirs,files in os.walk(self.path):
            for name in files: 
                if name.endswith(file_extension):    
                    file_path = os.path.join(root,name)
                    path_list.append(file_path)
        if len(path_list) == 0:
            self.error_msg(self.read_path.__name__,"read path function: csv file not found","check csv file")
        else: 
            self.path_list = path_list
            self.info_msg(self.read_path.__name__,f"found: {len(path_list)} file")

    def read_data(self):
        try:
            df = pd.read_csv(self.path_now,sep=",")
            df.dropna(inplace=True)

            df['mc_no'] = self.path_now.split("_")[-1].split(".")[0] # add filename to column

            self.df = df
            self.info_msg(self.read_data.__name__,f"csv to pd")
        except Exception as e:
            self.error_msg(self.read_data.__name__,"pd cannot read csv file",e)

    def query_df(self,query):
        try:
            connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+self.server+";DATABASE="+self.database+";UID="+self.user_login+";PWD="+self.password+""
            connection_url = engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
            engine1 = create_engine(connection_url)
            with engine1.begin() as conn:
                query_df = pd.read_sql_query(text(query), conn)
                self.info_msg(self.query_df.__name__,f"query df success")
                return query_df
        except Exception as e:
                self.error_msg(self.query_df.__name__,"cannot select with sql code",e)

    def query_duplicate(self):
        mc_no = self.path_now.split("_")[-1].split(".")[0]
        query =  """SELECT TOP(3000)
         CONVERT(VARCHAR, [occurred],20) AS 'occurred',
         CAST([mc_status] AS int),
         [mc_no] 
         FROM ["""+self.database+"""].[dbo].["""+self.table+"""] 
         where [mc_no] = '"""+mc_no+"""' 
         order by [registered_at] desc"""
        df = self.query_df(query)
        df['occurred'] = pd.to_datetime(df.occurred)
        return df
        
    def check_duplicate(self):
        try:
            df_from_db = self.query_duplicate()
            df = self.df
            df['occurred'] = pd.to_datetime(df.occurred)

            df_right_only = pd.merge(df_from_db,df , on = ["occurred","mc_no"], how = "right", indicator = True) 
            df_right_only = df_right_only[df_right_only['_merge'] == 'right_only'].drop(columns=['_merge'])
            if df_right_only.empty:              
                self.info_msg(self.check_duplicate.__name__,f"data is not new for update")
            else:
                self.info_msg(self.check_duplicate.__name__,f"we have data new")
                self.df_insert = df_right_only       
                return constant.STATUS_OK    
        except Exception as e:
            self.error_msg(self.check_duplicate.__name__,"cannot select with sql code",e)
    
    def df_to_db(self):
        #connect to db
        mcstatus_list = ['occurred','mc_status','mc_no']
        cnxn,cursor=self.conn_sql()
        try:
            df = self.df_insert
            for index, row in df.iterrows():
                value = None
                for i in range(len(mcstatus_list)):
                    address = mcstatus_list[i]
                    if value == None:
                        value = ",'"+str(row[address])+"'"
                    else:
                        value = value+",'"+str(row[address])+"'"
                
                insert_string = f"""
                INSERT INTO [{self.database}].[dbo].[{self.table}] 
                values(
                    getdate()
                    {value}
                    )
                    """  
                cursor.execute(insert_string)
                cnxn.commit()
            cursor.close()
            self.df_insert = None

            self.info_msg(self.df_to_db.__name__,f"insert data successfully")     
        except Exception as e:
            print('error: '+str(e))
            self.error_msg(self.df_to_db.__name__,"cannot insert df to sql",e)

    def run(self):
        self.stamp_time()
        self.read_path()
        for i in range(len(self.path_list)):
            self.path_now = self.path_list[i]
            self.read_data()
            if self.check_duplicate() == constant.STATUS_OK:
                  self.df_to_db()    
        self.ok_msg(self.df_to_db.__name__)

if __name__ == "__main__":
    print("must be run with main")
