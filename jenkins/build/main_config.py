import streamlit as st
import dotenv
import os
import time 
import pymssql
import utils.alert as alert
import json
import pandas as pd
import subprocess

from influxdb import InfluxDBClient
from stlib import mqtt
from utils.crontab_config import crontab_delete,crontab_every_minute,crontab_every_hr,crontab_read

def conn_sql(st,server,user_login,password,database):
        try:
            cnxn = pymssql.connect(server,user_login,password,database)
            st.success('SQLSERVER CONNECTED!', icon="✅")
            cnxn.close()
        except Exception as e:
            st.error('Error,Cannot connect sql server :'+str(e), icon="❌")

def create_table(st,server,user_login,password,database,table,table_columns):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor()
        # create table
        try:
            cursor.execute('''
            CREATE TABLE '''+table+''' (
                '''+table_columns+'''
                )
                ''')
            cnxn.commit()
            cursor.close()
            st.success('CREATE TABLE SUCCESSFULLY!', icon="✅")
            return True
        except Exception as e:
            if 'There is already an object named' in str(e):
                st.error('TABLE is already an object named ', icon="❌")
            elif 'Column, parameter, or variable' in str(e):
                st.error('define columns mistake', icon="❌")
            else:
                st.error('Error'+str(e), icon="❌")
            return False

def drop_table(st,server,user_login,password,database,table):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor()
        # create table
        try:
            cursor.execute(f'''DROP TABLE {table}''')
            cnxn.commit()
            cursor.close()
            st.success('DROP TABLE SUCCESSFULLY!', icon="✅")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def preview_sqlserver(st,server,user_login,password,database,table,mc_no,process):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor(as_dict=True)
        # create table
        try:
            cursor.execute(f'''SELECT TOP(20) * FROM {table} where mc_no = '{mc_no}' and process = '{process}' order by registered_at desc''')
            data=cursor.fetchall()
            cursor.close()
            if len(data) != 0:
                df=pd.DataFrame(data)
                st.dataframe(df,width=1500)
            else:
                st.error('Error: SQL SERVER NO DATA', icon="❌")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def preview_influx(st,influx_server,influx_user_login,influx_password,influx_database,column_names,mqtt_topic) :
      try:
            result_lists = []
            client = InfluxDBClient(influx_server, 8086,influx_user_login,influx_password,influx_database)
            query = f"select time,topic,{column_names} from mqtt_consumer where topic = '{mqtt_topic}' order by time desc limit 5"
            result = client.query(query)
            if list(result):
                query_list = list(result)[0]
                df = pd.DataFrame(query_list)
                df.time = pd.to_datetime(df.time).dt.tz_convert('Asia/Bangkok')
            
                st.dataframe(df,width=1500)
            else:
                st.error('Error: influx no data', icon="❌")
      except Exception as e:
          st.error('Error: '+str(e), icon="❌")

def log_sqlserver(st,server,user_login,password,database,table):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor(as_dict=True)
        # create table
        try:
            cursor.execute(f'''SELECT TOP(20) * FROM {table} order by registered_at desc''')
            data=cursor.fetchall()
            cursor.close()
            if len(data) != 0:
                df=pd.DataFrame(data)
                st.dataframe(df,width=2000)
            else:
                st.error('Error: SQL SERVER NO DATA', icon="❌")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def config_project():
    st.header("PROJECT")
    project_name = str(os.environ["TABLE"]).split("_")[-1]

    cols = st.columns(2)
    project_name_input = cols[0].text_input('Project Name', project_name)
    project_type_list = list(str(os.environ["PROJECT_TYPE_LIST"]).split(","))
    indexs= project_type_list.index(os.environ["PROJECT_TYPE"])
    project_type_value = cols[0].selectbox("Project type",project_type_list,placeholder="select project type...",index=indexs)

    os.environ["TABLE"] = "DATA_"+str(project_type_value)+"_"+str(project_name_input.upper())
    os.environ["TABLE_LOG"] = "LOG_"+str(project_type_value)+"_"+str(project_name_input.upper())
    os.environ["PROJECT_TYPE"] = project_type_value

    dotenv.set_key(dotenv_file,"TABLE",os.environ["TABLE"])
    dotenv.set_key(dotenv_file,"TABLE_LOG",os.environ["TABLE_LOG"])
    dotenv.set_key(dotenv_file,"PROJECT_TYPE",os.environ["PROJECT_TYPE"])

    cols[1].text("PREVIEW ")
    cols[1].text("TABLE NAME: "+str(os.environ["TABLE"]))
    cols[1].text("TABLE LOG NAME: "+str(os.environ["TABLE_LOG"]))

    st.markdown("---")

def config_mqtt():
    st.header("MQTT TOPIC REGISTRY")
    mqtt_registry = list(str(os.environ["MQTT_TOPIC"]).split(","))
    cols = st.columns(2)

    cols[1].text("PREVIEW ")
    cols[1].text("MQTT TOPIC REGISTRY: "+str(os.environ["MQTT_TOPIC"]))   

    cols[0].caption("Topic: division/process/machine_no")

    add_new_mqtt = cols[0].text_input("Add a new mqtt ","")
    add_new_mqtt_but = cols[0].button("Add MQTT", type="secondary")

    mqtt_value = None

    if add_new_mqtt and add_new_mqtt_but:
        mqtt_registry.append(add_new_mqtt)
        for i in range(len(mqtt_registry)):
            if mqtt_value == None:
                mqtt_value = mqtt_registry[i]
            else:
                mqtt_value = str(mqtt_value)+","+mqtt_registry[i]

        os.environ["MQTT_TOPIC"] = mqtt_value
        dotenv.set_key(dotenv_file,"MQTT_TOPIC",os.environ["MQTT_TOPIC"])

        st.success('Done!', icon="✅")
        time.sleep(0.5)
        st.experimental_rerun()
   
    option_mqtt = cols[0].selectbox(
                "Delete mqtt",
                mqtt_registry,
                index=None,
                placeholder="select sensor...",
                    )
    cols = st.columns(5)
    
    if option_mqtt:
        delete_mqtt = cols[0].button("Delete MQTT", type="primary")
        mqtt_value = None

        if delete_mqtt:
            if len(mqtt_registry)>1:
                mqtt_registry.remove(option_mqtt)
                for i in range(len(mqtt_registry)):
                    if mqtt_value == None:
                        mqtt_value = mqtt_registry[i]

                    else:
                        mqtt_value = str(mqtt_value)+","+mqtt_registry[i]

                os.environ["MQTT_TOPIC"] = mqtt_value
                dotenv.set_key(dotenv_file,"MQTT_TOPIC",os.environ["MQTT_TOPIC"])
                st.success('Deleted!', icon="✅")
                time.sleep(0.5)
                st.experimental_rerun()
            else:
                st.error('Cannot delete,sensor regisry must have at least one!', icon="❌")

    st.markdown("---")

def config_sensor_registry():
    st.header("SENSOR REGISTRY")
    sensor_registry = list(str(os.environ["COLUMN_NAMES"]).split(","))
    cols = st.columns(2)

    cols[1].text("PREVIEW ")
    cols[1].text("SENSOR REGISTRY: "+str(os.environ["COLUMN_NAMES"]))   
    cols[1].text("DATATYPE: "+str(os.environ["TABLE_COLUMNS"]))

    add_new_input = cols[0].text_input("Add a new sensor ","")
    add_new_but = cols[0].button("ADD SENSOR", type="secondary")
    value = None
    table_column_value = "registered_at datetime,mc_no varchar(10),process varchar(10)"

    if add_new_input and add_new_but:
        sensor_registry.append(add_new_input)
        for i in range(len(sensor_registry)):
            if value == None :
                value = sensor_registry[i]
         
            else:
                value = str(value)+","+sensor_registry[i]
            table_column_value = table_column_value+","+sensor_registry[i] + " varchar(10)"
     
        os.environ["TABLE_COLUMNS"] = table_column_value
        os.environ["COLUMN_NAMES"] = value

        dotenv.set_key(dotenv_file,"COLUMN_NAMES",os.environ["COLUMN_NAMES"])
        dotenv.set_key(dotenv_file,"TABLE_COLUMNS",os.environ["TABLE_COLUMNS"])

        st.success('Done!', icon="✅")
        time.sleep(0.5)
        st.experimental_rerun()
   
    option = cols[0].selectbox(
   "Delete sensor",
   sensor_registry,
   index=None,
   placeholder="select sensor...",
    )
    cols = st.columns(5)
    
    if option:
        delete = cols[0].button("Delete sensor", type="primary")
        value = None
        table_column_value = "registered_at datetime,mc_no varchar(10),process varchar(10)"

        if delete:
            if len(sensor_registry)>1:
                sensor_registry.remove(option)
                for i in range(len(sensor_registry)):
                    if value == None :
                        value = sensor_registry[i]
                    else:
                        value = str(value)+","+sensor_registry[i]
                    table_column_value = table_column_value+","+sensor_registry[i] + " varchar(10)"

                os.environ["TABLE_COLUMNS"] = table_column_value
                os.environ["COLUMN_NAMES"] = value
                dotenv.set_key(dotenv_file,"COLUMN_NAMES",os.environ["COLUMN_NAMES"])
                dotenv.set_key(dotenv_file,"TABLE_COLUMNS",os.environ["TABLE_COLUMNS"])
                st.success('Deleted!', icon="✅")
                time.sleep(0.5)
                st.experimental_rerun()
            else:
                st.error('Cannot delete,sensor regisry must have at least one!', icon="❌")
    st.markdown("---")

def config_db_connect(env_headers):
        total_env_list = None
        if env_headers == "SQLSERVER":
            total_env_list = sql_server_env_lists = ["SERVER","DATABASE","USER_LOGIN","PASSWORD"]
        elif env_headers == "INFLUXDB":
            total_env_list = influxdb_env_lists = ["INFLUX_SERVER","INFLUX_DATABASE","INFLUX_USER_LOGIN","INFLUX_PASSWORD"]
        else :
            st.error("don't have the connection")

        if total_env_list is not None:
            st.header(env_headers)
            cols = st.columns(len(total_env_list))
            for j in range(len(total_env_list)):
                param = total_env_list[j]
                if "PASSWORD" in param or "TOKEN" in param:
                    type_value = "password"
                else:
                    type_value = "default"
                os.environ[param] = cols[j].text_input(param,os.environ[param],type=type_value)
                dotenv.set_key(dotenv_file,param,os.environ[param])

            cols = st.columns(2) 

            if env_headers == "SQLSERVER":

                sql_check_but = cols[0].button("CONECTION CHECK",key="sql_check")
                if sql_check_but:
                    conn_sql(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"])

            elif env_headers == "INFLUXDB":
                influx_check_but = cols[0].button("CONECTION CHECK",key="influx_check")
                if influx_check_but:
                    try:
                        client = InfluxDBClient(os.environ["INFLUX_SERVER"], 8086, os.environ["INFLUX_USER_LOGIN"], os.environ["INFLUX_PASSWORD"], os.environ["INFLUX_DATABASE"])
                        result = client.query('select * from mqtt_consumer order by time limit 1')
                        st.success('INFLUXDB CONNECTED!', icon="✅")
                    except Exception as e:
                        st.error("Error :"+str(e))
            else:
                st.error('Dont have the connection!', icon="❌")

            st.markdown("---")

def config_initdb():
        st.header("DB STATUS")
        initial_db_value = os.environ["INITIAL_DB"]
        if initial_db_value == "False":
            st.error('DB NOT INITIAL', icon="❌")
            st.write("PLEASE CONFIRM CONFIG SETUP BEFORE INITIAL")
            initial_but = st.button("INITIAL")
            if initial_but:
                if os.environ["PROJECT_TYPE"] == "PRODUCTION":
                    table_column = "TABLE_COLUMNS"
                elif os.environ["PROJECT_TYPE"] == "MCSTATUS":
                    table_column = "MCSTATUS_TABLE_COLUMNS"
                elif os.environ["PROJECT_TYPE"] == "ALARMLIST":
                    table_column = "ALARMLIST_TABLE_COLUMNS"
                else:
                    table_column = None
                if table_column is not None:
                    result_1 = create_table(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE"],os.environ[table_column])
                    result_2 = create_table(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG"],os.environ["TABLE_COLUMNS_LOG"])
                    if result_1 and result_2 is not False:
                        os.environ["INITIAL_DB"] = "True"
                        dotenv.set_key(dotenv_file,"INITIAL_DB",os.environ["INITIAL_DB"])
                        st.experimental_rerun()
                else:
                    st.error('UNKNOWN PROJECT TYPE', icon="❌")
        else:
            st.success('DB CREATED!', icon="✅")
            with st.expander("DELETE DB"):
                st.error('DANGER ZONE!!!!! PLEASE BACKUP DB BEFORE REMOVE')
                st.write("DELETE TABLE:  "+os.environ["TABLE"])
                st.write("DELETE TABLE:  "+os.environ["TABLE_LOG"])
                remove_input = st.text_input("PASSWORD","",type="password")
                remove_but = st.button("REMOVE DB")
                if remove_but:
                    if remove_input=="mic@admin":
                        drop_table(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE"])
                        drop_table(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG"])
                        os.environ["INITIAL_DB"] = "False"
                        dotenv.set_key(dotenv_file,"INITIAL_DB",os.environ["INITIAL_DB"])
                        st.success('Deleted!', icon="✅")
                        time.sleep(0.5)
                        st.experimental_rerun()
                    else:
                        st.error('Cannot delete,password mistake!', icon="❌")

def read_path(path):
        path_list = []
        file_extension = '.txt'
        for root,dirs,files in os.walk(path):
            for name in files: 
                if name.endswith(file_extension):    
                    file_path = os.path.join(root,name)
                    path_list.append(file_path)
        if len(path_list) == 0:
            st.error('read path function: txt file not found!', icon="❌")
        return path_list

def read_txt(path_now):
        try:
            df = pd.read_csv(path_now,sep=",")
            df.dropna(inplace=True)
            df['mc_no'] = path_now.split("_")[-1].split(".")[0] # add filename to column
            st.dataframe(df,width=2000)
  
        except Exception as e:
            st.error('Cannot read txt file', icon="❌")
    

def mcstatus_path():
    st.header("MCSTATUS TXT FILE PATH")
    mcstatus_path = str(os.environ["MCSTATUS_PATH"])

    cols = st.columns(2)
    mcstatus_input = cols[0].text_input('FLODER PATH', mcstatus_path)

    os.environ["MCSTATUS_PATH"] = mcstatus_input
    dotenv.set_key(dotenv_file,"MCSTATUS_PATH",os.environ["MCSTATUS_PATH"])

    mcstatus_floder_path_but = st.button("PREVIEW")
    if mcstatus_floder_path_but:
        mcstatus_read_path_value = read_path(os.environ["MCSTATUS_PATH"])
        if mcstatus_read_path_value:
            st.write(mcstatus_read_path_value)
        

    cols[1].text("PREVIEW ")
    cols[1].text("MCSTATUS PATH: "+str(os.environ["MCSTATUS_PATH"]))

    st.markdown("---")

def alarmlist_path():
    st.header("ALARMLIST TXT FILE PATH")
    alarmlist_path = str(os.environ["ALARMLIST_PATH"])

    cols = st.columns(2)
    alarmlist_input = cols[0].text_input('FLODER PATH', alarmlist_path)

    os.environ["ALARMLIST_PATH"] = alarmlist_input
    dotenv.set_key(dotenv_file,"ALARMLIST_PATH",os.environ["ALARMLIST_PATH"])

    alarmlist_floder_path_but = st.button("PREVIEW")
    if alarmlist_floder_path_but:
        alarmlist_read_path_value = read_path(os.environ["ALARMLIST_PATH"])
        if alarmlist_read_path_value:
            st.write(alarmlist_read_path_value)

    cols[1].text("PREVIEW ")
    cols[1].text("ALARMLIST PATH: "+str(os.environ["ALARMLIST_PATH"]))

    st.markdown("---")

def line_alert():
        st.header("LINE NOTIFY")
        line_notify_flag_value = os.environ["LINE_NOTIFY_FLAG"]

        line_notify_token_input = st.text_input("LINE NOTIFY TOKEN",os.environ["LINE_NOTIFY_TOKEN"],type="password")
 
        if line_notify_token_input:
            alert_toggle = st.toggle('Activate line notify feature',value=eval(line_notify_flag_value))

            if alert_toggle:
                line_notify_flag_value = 'True'
                os.environ["LINE_NOTIFY_FLAG"] = line_notify_flag_value
                st.success('LINE NOTIFY ACTIVED!', icon="✅")
            else:
                line_notify_flag_value = 'False'
                os.environ["LINE_NOTIFY_FLAG"] = line_notify_flag_value
                st.error('LINE NOTIFY DEACTIVED!', icon="❌")

            os.environ["LINE_NOTIFY_TOKEN"] = line_notify_token_input
            dotenv.set_key(dotenv_file,"LINE_NOTIFY_FLAG",os.environ["LINE_NOTIFY_FLAG"])
            dotenv.set_key(dotenv_file,"LINE_NOTIFY_TOKEN",os.environ["LINE_NOTIFY_TOKEN"])
        
            st.markdown("---")

            if alert_toggle:
                st.write("ALERT CONNECTION CHECK")
                cols = st.columns(2) 

                cols[0].caption("LINE NOTIFY CHECK")
                line_check_but = cols[0].button("CHECK",key="line_notify_check")
        
                if line_check_but:
                    status = alert.line_notify(os.environ["LINE_NOTIFY_TOKEN"],"Test send from "+os.environ["TABLE"]+" project")
                    status_object = json.loads(status)
                    if status_object["status"] == 401:
                        st.error("Error: "+status_object["message"], icon="❌")
                    else:
                        st.success('SUCCESSFUL SENDING LINE NOTIFY!', icon="✅")

                st.markdown("---")

def dataflow_production_mqtt():
        st.caption("MQTT")
        mqtt_broker_input = st.text_input('MQTT Broker', os.environ["MQTT_BROKER"])
        os.environ["MQTT_BROKER"] = mqtt_broker_input
        dotenv.set_key(dotenv_file,"MQTT_BROKER",os.environ["MQTT_BROKER"])
        mqtt_registry = list(str(os.environ["MQTT_TOPIC"]).split(","))

        preview_mqtt_selectbox = st.selectbox(
                "mqtt topic",
                mqtt_registry,
                index=None,
                placeholder="select topic...",
                key='preview_mqtt'
                    )

        if preview_mqtt_selectbox:
            cols = st.columns(9)
            preview_mqtt_but = cols[0].button("CONNECT",key="preview_mqtt_but")
            stop_mqtt_but = cols[1].button("STOP",key="stop_mqtt_but",type="primary")
            if preview_mqtt_but:
                mqtt.run_subscribe(st,os.environ["MQTT_BROKER"],1883,preview_mqtt_selectbox)
            if stop_mqtt_but:
                mqtt.run_publish(os.environ["MQTT_BROKER"],1883,preview_mqtt_selectbox)

        st.markdown("---")

def dataflow_production_influx():
        st.caption("INFLUXDB")
        mqtt_registry = list(str(os.environ["MQTT_TOPIC"]).split(","))
        preview_influx_selectbox = st.selectbox(
                "mqtt topic",
                mqtt_registry,
                index=None,
                placeholder="select topic...",
                key='preview_influx'
                    )

        if preview_influx_selectbox:
            preview_influx_but = st.button("QUERY",key="preview_influx_but")
        
            if preview_influx_but:
                preview_influx(st,os.environ["INFLUX_SERVER"],os.environ["INFLUX_USER_LOGIN"],os.environ["INFLUX_PASSWORD"],os.environ["INFLUX_DATABASE"],os.environ["COLUMN_NAMES"],preview_influx_selectbox)

        st.markdown("---")

def dataflow_test():
        st.caption("TEST RUN THE PROGRAM")
        test_run_but = st.button("TEST",key="test_run_but")
        if test_run_but:
            try:
                result = subprocess.check_output(['python', 'main.py'])
                st.write(result.decode('UTF-8'))
                st.success('TEST RUN SUCCESS!', icon="✅")
            except Exception as e:
                st.error("Error :"+str(e))
        st.markdown("---")

def preview_production_sqlserver(server,user_login,password,database,table,mc_no,process):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor(as_dict=True)
        # create table
        try:
            cursor.execute(f'''SELECT TOP(20) * FROM {table} where mc_no = '{mc_no}' and process = '{process}' order by registered_at desc''')
            data=cursor.fetchall()
            cursor.close()
            if len(data) != 0:
                df=pd.DataFrame(data)
                st.dataframe(df,width=1500)
            else:
                st.error('Error: SQL SERVER NO DATA', icon="❌")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def dataflow_production_sql():
        st.caption("SQLSERVER")

        mqtt_registry = list(str(os.environ["MQTT_TOPIC"]).split(","))
        preview_sqlserver_selectbox = st.selectbox(
                "mqtt topic",
                mqtt_registry,
                index=None,
                placeholder="select topic...",
                key='preview_sqlserver'
                    )

        if preview_sqlserver_selectbox:
            preview_sqlserver_but = st.button("QUERY",key="preview_sqlserver_but")
        
            if preview_sqlserver_but:
                mc_no = preview_sqlserver_selectbox.split("/")[-1]
                process = preview_sqlserver_selectbox.split("/")[-2]
                preview_production_sqlserver(os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE"],mc_no,process)
        st.markdown("---")

def dataflow_mcstatus_file():
    st.caption("MCSTATUS FILE")
    mcstatus_read_path_value = read_path(os.environ["MCSTATUS_PATH"])
    preview_mcstatus_path_selectbox = st.selectbox("txt file",mcstatus_read_path_value,index=None,placeholder="select txt file...",key='preview_mcstatus_file')
    if preview_mcstatus_path_selectbox:
        read_txt(preview_mcstatus_path_selectbox)
    st.markdown("---")

def dataflow_alarmlist_file():
    st.caption("ALARMLIST FILE")
    alarmlist_read_path_value = read_path(os.environ["ALARMLIST_PATH"])
    preview_alarmlist_path_selectbox = st.selectbox("txt file",alarmlist_read_path_value,index=None,placeholder="select txt file...",key='preview_alarmlist_file')
    if preview_alarmlist_path_selectbox:
        read_txt(preview_alarmlist_path_selectbox)
    st.markdown("---")

def dataflow_alarmlist_sql():
    st.caption("SQLSERVER")
    alarmlist_read_path_value = read_path(os.environ["ALARMLIST_PATH"])
    preview_sqlserver_selectbox = st.selectbox("txt file",alarmlist_read_path_value,index=None,placeholder="select txt file...",key='preview_alarmlist_sql')

    if preview_sqlserver_selectbox:
            preview_sqlserver_but = st.button("QUERY",key="preview_alarmlist_sqlserver_but")
        
            if preview_sqlserver_but:
                mc_no = preview_sqlserver_selectbox.split("_")[-1].split(".")[0]
                preview_sqlserver(os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE"],mc_no)
    st.markdown("---")

def preview_sqlserver(server,user_login,password,database,table,mc_no):
        #connect to db
        cnxn = pymssql.connect(server,user_login,password,database)
        cursor = cnxn.cursor(as_dict=True)
        # create table
        try:
            cursor.execute(f'''SELECT TOP(20) * FROM {table} where mc_no = '{mc_no}' order by registered_at desc''')
            data=cursor.fetchall()
            cursor.close()
            if len(data) != 0:
                df=pd.DataFrame(data)
                st.dataframe(df,width=1500)
            else:
                st.error('Error: SQL SERVER NO DATA', icon="❌")
        except Exception as e:
            st.error('Error'+str(e), icon="❌")

def dataflow_mcstatus_sql():
    st.caption("SQLSERVER")
    mcstatus_read_path_value = read_path(os.environ["MCSTATUS_PATH"])
    preview_sqlserver_selectbox = st.selectbox("txt file",mcstatus_read_path_value,index=None,placeholder="select txt file...",key='preview_mcstatus_sql')

    if preview_sqlserver_selectbox:
            preview_sqlserver_but = st.button("QUERY",key="preview_mcstatus_sqlserver_but")
        
            if preview_sqlserver_but:
                mc_no = preview_sqlserver_selectbox.split("_")[-1].split(".")[0]
                preview_sqlserver(os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE"],mc_no)
    st.markdown("---")

def logging():
    st.header("LOG")
    if os.environ["INITIAL_DB"] == "True":
        log_sqlserver(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG"])
    else:
        st.error('DB NOT INITIAL', icon="❌")

def main_layout():
    st.set_page_config(
            page_title="MACHINE DATA TO DB CONFIG",
            page_icon="🗃",
            layout="wide",
            initial_sidebar_state="expanded",
        )

    st.title("MACHINE DATA TO DB CONFIG")

    #tab1, tab2 , tab3 ,tab4 , tab5 , tab6 , tab7 = st.tabs(["⚙️ PROJECT CONFIG", "🔑 DB CONNECTION", "📂 DB CREATE", "🔔 ALERT", "🔍 DATAFLOW PREVIEW","📝LOG","🕞SCHEDULE"])
    tab1, tab2 , tab3 ,tab4 , tab5 , tab6 = st.tabs(["⚙️ PROJECT CONFIG", "🔑 DB CONNECTION", "📂 DB CREATE", "🔔 ALERT", "🔍 DATAFLOW PREVIEW","📝LOG"])
    
    with tab1:
        config_project()
        project_type = os.environ["PROJECT_TYPE"]
        if project_type == 'PRODUCTION':
            config_mqtt()
            config_sensor_registry()
        elif project_type == 'MCSTATUS':
            mcstatus_path()
        elif project_type == 'ALARMLIST':
            alarmlist_path()
        else:
            st.error('ERROR: UNKNOWN PROJECT TYPE!', icon="❌")

    with tab2:
        config_db_connect("SQLSERVER")
        if os.environ["PROJECT_TYPE"] == 'PRODUCTION':
            config_db_connect("INFLUXDB")

    with tab3:
        config_initdb()

    with tab4:
        line_alert()

    with tab5:
            st.header("DATAFLOW PREVIEW")
            if project_type == 'PRODUCTION':
                dataflow_production_mqtt()
                dataflow_production_influx()
                dataflow_test()
                dataflow_production_sql()

            elif project_type == 'MCSTATUS':
                dataflow_mcstatus_file()
                dataflow_test()
                dataflow_mcstatus_sql()
                
            elif project_type == 'ALARMLIST':
                dataflow_alarmlist_file()
                dataflow_test()
                dataflow_alarmlist_sql()

    with tab6:
        logging()

    # with tab7:
    #     crontab_value = st.selectbox('Select Schedule',('Every 1 minute', 'Hourly'))
    #     crontab_but = st.button("SUBMIT")
    #     st.error("DANGER!!! RESTART CONTAINTER AFTER SUBMIT")
        
    #     st.markdown("---")
    #     st.subheader("READ CRONTAB")
    #     st.markdown("---")
    #     st.write(crontab_read())
    #     st.markdown("---")
    #     if crontab_but:
    #             if crontab_value == 'Every 1 minute':
    #                 crontab_delete()
    #                 crontab_every_minute()
    #                 st.experimental_rerun()
    #             elif crontab_value == 'Hourly':
    #                 crontab_delete()
    #                 crontab_every_hr()
    #                 st.experimental_rerun()
    #             else:
    #                 st.error("Error: crontab unknown")
        
if __name__ == "__main__":
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)
    main_layout()
