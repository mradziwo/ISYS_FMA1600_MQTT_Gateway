import paho.mqtt.client as mqtt
import time
import json
import socket
import yaml
from datetime import datetime  


class communication:
    
    def start(self,config):
        raise NotImplementedError('users must define start(configuration) to use this base class')
    def stop(self):
        raise NotImplementedError('users must define stop() to use this base class')
    def write(self,message):
        raise NotImplementedError('users must define write() to use this base class')
    def read(self,n):
        raise NotImplementedError('users must define read() to use this base class')
        
class communicationConfig:
    def __init__(self, ip, port):
        self.ip=ip
        self.port=port
    
class dummy_communication(communication):
    def start(self, config):
        print ("configured")
    def stop(self):
        print ("stopped")
    def write(self,message):
        print ("Message to write:", message)
    def read(self,n):
        t="A +042.81 +022.70 +000000 +000000     CH4\r"
        req=t[:n]
        print ("read request returns:", req)
        return(req)
    
class RSOverMoxa(communication):
    def start(self,config):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self._socket.connect(("192.168.20.119", 4001))
        self._socket.connect((config.ip, config.port))
        
    def stop(self):
        self._socket.close()
        
    def write(self, message):
        self._socket.sendall(message)
    
    def read(self,n):
        message=""
        while len(message) <n:
            message = message+self._socket.recv(1024).decode()
        return (message)




class FlowMeter:
    
    def tare(self):
        raise NotImplementedError('users must define tare() to use this base class')  
    def poll(self):
        raise NotImplementedError('users must define poll() to use this base class') 
        
    def start(self, config):
        raise NotImplementedError('users must define start() to use this base class') 
        
    def stop(self):
        raise NotImplementedError('users must define stop() to use this base class') 

class FMA1600(FlowMeter):
    
    TareString="A$$V\r".encode()
    QuerryString="A\r".encode()
    ReplyLen=42
    
    def __init__ (self, channel,config):
        self._channel=channel()
        self.start(config)
    
    def start(self,config):
        self._channel.start(config)
        
    def stop(self):
        self._channel.stop()
        
    def tare(self):
        
        self._channel.write(self.TareString)
    
    def poll(self):
        self._channel.write(self.QuerryString)
        reply=self._channel.read(self.ReplyLen)
        w= reply.split(" ")
        p=0.0689476*float(w[1])
        t=float(w[2])
        q=float(w[3])
        mq=float(w[4])
        return(p, t, mq)



with open("config.yml", 'r') as stream:
    try:
        conf=yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)
        
MQTTIP=conf["MQTT"]["IP"]
MQTTPort=conf["MQTT"]["Port"]
MQTTUser=conf["MQTT"]["User"]
MQTTPassword=conf["MQTT"]["Password"]
MQTTKeepAlive=conf["MQTT"]["KeepAlive"]
MQTTRootPath=conf["MQTT"]["rootPath"]

MOXAIP=conf["Moxa"]["IP"]
MOXAPort=conf["Moxa"]["Port"]

ScanRate=conf["Settings"]["ScanRate"]




device_root=MQTTRootPath
config=communicationConfig(MOXAIP,MOXAPort)
FlowMeterDevice=FMA1600(RSOverMoxa,config);

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    line=datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tConnected to "+MQTTIP+":"+str(MQTTPort)+" with result code "+str(rc)
    print (line)
    client.publish(device_root+"/Info/Status", "Online" )
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(device_root+"/Tare")
    line=datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tSubscribed to: "+device_root+"/Tare"
    print (line)
    client.subscribe(device_root+"/Disconnect")
    line=datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tSubscribed to: "+device_root+"/Disconnect"
    print (line)

    
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

    
def Tare_callback(client, userdata, message):
    if message.payload == 1:
        FlowMeterDevice.tare()
        
    
def do_disconnect(client, userdata, message):
    client.disconnect()
    print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3],"]\tClient disconnected from server")

    
username=MQTTUser
password=MQTTPassword

client = mqtt.Client(client_id="OMEGAREADER")
client.username_pw_set(username, password=None)
client.will_set(device_root+"/Info/Status", payload="Offline", qos=0, retain=True)
client.on_connect = on_connect
client.on_message = on_message

client.message_callback_add(device_root+"/Tare", Tare_callback)
client.message_callback_add(device_root+"/Disconnect", do_disconnect)

client.connect(MQTTIP, MQTTPort, MQTTKeepAlive)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_start()
for i in range (0,10):
    p, t, mq = FlowMeterDevice.poll()
    pack={"Pressure":{"Value":p,"Unit":"bar"}, "Temperature":{"Value":t, "Unit":"°C"},"Flow":{"Value":mq, "Unit": "nlpm"}}
    client.publish(device_root+"/Data/All", json.dumps(pack) )
    client.publish(device_root+"/Data/Pressure", json.dumps(pack["Pressure"]))
    client.publish(device_root+"/Data/Temperature", json.dumps(pack["Temperature"]))
    y=client.publish(device_root+"/Data/Flow", json.dumps(pack["Flow"]))
    if y[0] == 4:
        break
    time.sleep(ScanRate)
    
client.disconnect()
FlowMeterDevice.stop()
print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3], "]\tProcess stopped")