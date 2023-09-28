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


    def __init__(self,server,database,user_login,password,table,table_columns,table_log,table_columns_log,line_notify_token,influx_server,influx_database,influx_user_login,influx_password,column_names,mqtt_topic,initial_db,line_notify_flag):
        self.server = server
        self.database = database
        self.user_login = user_login
        self.password = password
        self.table_log = table_log
        self.table = table
        self.table_columns = table_columns
        self.table_columns_log = table_columns_log
        self.df = None
        self.df_insert = None
        self.line_notify_token = line_notify_token
        self.influx_server = influx_server
        self.influx_database = influx_database
        self.influx_user_login = influx_user_login
        self.influx_password = influx_password
        self.column_names = column_names
        self.mqtt_topic = mqtt_topic
        self.initial_db = initial_db
        self.line_notify_flag = line_notify_flag

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
 

class INFLUX_TO_SQLSERVER(PREPARE):
    def __init__(self,server,database,user_login,password,table,table_columns,table_log,table_columns_log,influx_server,influx_database,influx_user_login,influx_password,column_names,mqtt_topic,initial_db,line_notify_flag,line_notify_token=None):
        super().__init__(server,database,user_login,password,table,table_columns,table_log,table_columns_log,line_notify_token,influx_server,influx_database,influx_user_login,influx_password,column_names,mqtt_topic,initial_db,line_notify_flag)        

    def lastone(self) :
      try:
          result_lists = []
          client = InfluxDBClient(self.influx_server, 8086, self.influx_user_login,self.influx_password, self.influx_database)
          mqtt_topic_value = list(str(self.mqtt_topic).split(","))
          for i in range(len(mqtt_topic_value)):
              query = f"select time,topic,{self.column_names} from mqtt_consumer where topic = '{mqtt_topic_value[i]}' order by time desc limit 1"
              result = client.query(query)
              if list(result):
                result = list(result)[0][0]
                result_lists.append(result)
                result_df = pd.DataFrame.from_dict(result_lists)
                self.df = result_df
              else:
                print("influx no data")
      except Exception as e:
          self.error_msg(self.lastone.__name__,"cannot query influxdb",e)
    
    def edit_col(self):
        try:
            df = self.df.copy()
            df_split = df['topic'].str.split('/', expand=True)
            df['mc_no'] = df_split[2].values
            df['process'] = df_split[1].values
            df.drop(columns=['topic'],inplace=True)
            df.rename(columns = {'time':'data_timestamp'}, inplace = True)
            df["data_timestamp"] =   pd.to_datetime(df["data_timestamp"]).dt.tz_convert(None)
            df["data_timestamp"] = df["data_timestamp"] + pd.DateOffset(hours=7)    
            df["data_timestamp"] = df['data_timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            self.df_insert = df
        except Exception as e:
            self.error_msg(self.edit_col.__name__,"cannot edit dataframe data",e)

    def df_to_db(self):
        #connect to db
        init_list = ['mc_no','process']
        insert_db_value = self.column_names.split(",")
        col_list = init_list+insert_db_value

        cnxn,cursor=self.conn_sql()
      
        try:
            df = self.df_insert
            for index, row in df.iterrows():
                value = None
                for i in range(len(col_list)):
                    address = col_list[i]
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
        if self.initial_db == 'True':
            self.lastone()
            self.edit_col()
            self.df_to_db()
            self.ok_msg(self.df_to_db.__name__)
        else:
            print("db is not initial yet")


if __name__ == "__main__":
    print("must be run with main")
