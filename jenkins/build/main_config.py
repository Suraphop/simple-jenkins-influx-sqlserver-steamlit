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

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

st.set_page_config(
        page_title="MACHINE DATA TO DB CONFIG",
        page_icon="🗃",
        layout="wide",
        initial_sidebar_state="expanded",
    )

st.title("MACHINE DATA TO DB CONFIG")

tab1, tab2 , tab3 ,tab4 , tab5 , tab6 = st.tabs(["⚙️ PROJECT CONFIG", "🔑 DB CONNECTION", "📂 DB CREATE", "🔔 ALERT", "🔍 DATAFLOW PREVIEW","📝LOG"])

with tab1:
        
    env_headers = ["PROJECT NAME","SENSOR REGISTRY"]
    project_env_lists = ["TABLE","TABLE_LOG"]
    sensor_env_lists = ["TABLE_COLUMNS","COLUMN_NAMES"]
    total_env_list = [project_env_lists,sensor_env_lists]
    
    st.header("PROJECT NAME")
    project_name = str(os.environ["TABLE"]).split("_")[1]
    cols = st.columns(2)
    project_name_input = cols[0].text_input('Project Name', project_name)
    os.environ[project_env_lists[0]] = "data_"+project_name_input
    os.environ[project_env_lists[1]] = "log_"+project_name_input
    dotenv.set_key(dotenv_file,project_env_lists[0],os.environ[project_env_lists[0]])
    dotenv.set_key(dotenv_file,project_env_lists[1],os.environ[project_env_lists[1]])

    cols[1].text("PREVIEW ")
    cols[1].text("TABLE NAME: "+str(os.environ["TABLE"]))
    cols[1].text("TABLE LOG NAME: "+str(os.environ["TABLE_LOG"]))

    st.markdown("---")

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

with tab2:
    env_headers = ["SQL SERVER","INFLUXDB"]
    sql_server_env_lists = ["SERVER","DATABASE","USER_LOGIN","PASSWORD"]
    influxdb_env_lists = ["INFLUX_SERVER","INFLUX_DATABASE","INFLUX_USER_LOGIN","INFLUX_PASSWORD"]
    total_env_list = [sql_server_env_lists,influxdb_env_lists]

    for i in range(len(env_headers)):
        st.header(env_headers[i])
        cols = st.columns(len(total_env_list[i]))
        for j in range(len(total_env_list[i])):
            param = total_env_list[i][j]
            if "PASSWORD" in param or "TOKEN" in param:
                type_value = "password"
            else:
                type_value = "default"
            os.environ[param] = cols[j].text_input(param,os.environ[param],type=type_value)
            dotenv.set_key(dotenv_file,param,os.environ[param])
    
    st.markdown("---")

    st.write("DB CONNECTION CHECK")
    cols = st.columns(2) 

    cols[0].caption(env_headers[0])
    sql_check_but = cols[0].button("CHECK",key="sql_check")

    cols[1].caption(env_headers[1])
    influx_check_but = cols[1].button("CHECK",key="influx_check")

    if sql_check_but:
        conn_sql(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"])
    if influx_check_but:
        try:
            client = InfluxDBClient(os.environ["INFLUX_SERVER"], 8086, os.environ["INFLUX_USER_LOGIN"], os.environ["INFLUX_PASSWORD"], os.environ["INFLUX_DATABASE"])
            result = client.query('select * from mqtt_consumer order by time limit 1')
            st.success('INFLUXDB CONNECTED!', icon="✅")
        except Exception as e:
            st.error("Error :"+str(e))
    st.markdown("---")


with tab3:
        st.header("DB STATUS")
        initial_db_value = os.environ["INITIAL_DB"]
        if initial_db_value == "False":
            st.error('DB NOT INITIAL', icon="❌")
            st.write("PLEASE CONFIRM CONFIG SETUP BEFORE INITIAL")
            initial_but = st.button("INITIAL")
            if initial_but:
                result_1 = create_table(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE"],os.environ["TABLE_COLUMNS"])
                result_2 = create_table(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG"],os.environ["TABLE_COLUMNS_LOG"])
                if result_1 and result_2 is not False:
                    os.environ["INITIAL_DB"] = "True"
                    dotenv.set_key(dotenv_file,"INITIAL_DB",os.environ["INITIAL_DB"])
                    st.experimental_rerun()
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

with tab4:
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

with tab5:
        st.header("DATAFLOW PREVIEW")
        st.caption("MQTT")
        mqtt_broker_input = st.text_input('MQTT Broker', os.environ["MQTT_BROKER"])
        os.environ["MQTT_BROKER"] = mqtt_broker_input
        dotenv.set_key(dotenv_file,"MQTT_BROKER",os.environ["MQTT_BROKER"])

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

        st.caption("INFLUXDB")
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

        st.caption("SQLSERVER")

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
                preview_sqlserver(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE"],mc_no,process)
        st.markdown("---")

with tab6:
    st.header("LOG")
    if os.environ["INITIAL_DB"] == "True":
        log_sqlserver(st,os.environ["SERVER"],os.environ["USER_LOGIN"],os.environ["PASSWORD"],os.environ["DATABASE"],os.environ["TABLE_LOG"])