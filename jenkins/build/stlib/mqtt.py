
import random
import time

from paho.mqtt import client as mqtt_client

def connect_mqtt(client_id,broker,port):
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            #print("Connected to MQTT Broker!")
            pass
        else:
            #print("Failed to connect, return code"+rc)
            pass

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client,topic):

        msg = f"True"
        result = client.publish(topic, msg)
        status = result[0]
        if status == 0:
            #print(f"Send `{msg}` to topic `{topic}`")
            pass
        else:
            #print(f"Failed to send message to topic {topic}")
            pass

def run_publish(broker,port,topic):
    topic = "steamlit/"+str(topic)
    client_id = f'publish-{random.randint(0, 1000)}'
    client = connect_mqtt(client_id,broker,port)
    client.loop_start()
    publish(client,topic)
    client.loop_stop()
    

def subscribe(st,client,topic):
    def on_message(client, userdata, msg):
        #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        st.write(f"{msg.payload.decode()} , received from {msg.topic}")
        if msg.topic == stop_mqtt:
            if msg.payload.decode() == "True":
                client.disconnect()
                client.loop_stop() 
               
    stop_mqtt = "steamlit/"+str(topic)
    client.subscribe(topic) 
    client.subscribe(stop_mqtt)

    client.on_message = on_message

def run_subscribe(st,broker,port,topic):
    client_id = f'publish-{random.randint(0, 1000)}'
    client = connect_mqtt(client_id,broker,port)
    subscribe(st,client,topic)
    st.write("subscribe topic successful!!")
    client.loop_forever()

if __name__ == '__main__':
    #run_publish("192.168.100.11",1883,"topic")
    run_subscribe("192.168.100.11",1883,"topic")