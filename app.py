import paho.mqtt.client as mqtt
import time
import json
import socket
import yaml
from datetime import datetime  
import shutil
import sys
import threading


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
        try:
            self._socket.connect((config.ip, config.port))
            print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tConnected to RS485 gateway: "+config.ip+":",config.port)
        except socket.error as e:
            print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tError while connecting :: %s" % e)
        sys.stdout.flush()
        

        
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
    
    mutex = threading.Lock()
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
        print ("Tare request")  
        with self.mutex:
            self._channel.write(self.TareString)


    
    def poll(self):
        with self.mutex:
            self._channel.write(self.QuerryString)
            reply=self._channel.read(self.ReplyLen)
        w= reply.split(" ")
        p=0.0689476*float(w[1])
        t=float(w[2])
        mq=float(w[4])
        P=float(w[4])*0.54644058                    #nlpm to kW @CH4 conversion
        return(p, t, mq, P)





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
    sys.stdout.flush()
    
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

    
def Tare_callback(client, userdata, message):
    print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tClient received Tare request via "+device_root+"/Tare"+" PAYLOAD: "+str(message.payload))
    sys.stdout.flush()
    if message.payload == b'1':
        print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tTare request processed!")
        sys.stdout.flush()
        FlowMeterDevice.tare()
        
def do_disconnect(client, userdata, message):
    client.disconnect()
    print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tClient received disconnected request via "+device_root+"/Disconnect")
    sys.stdout.flush()



if __name__ == "__main__":
    try:
        with open("./config/config.yaml", 'r') as stream:
            conf=yaml.load(stream)
    except yaml.YAMLError as exc:
        print ("Error processing configuration file")
        print(exc)
        sys.stdout.flush()
    except IOError as exc:
        if exc.errno == 2:
            print ("Configuration file \\config\\config.yaml not found. Substituting with default configuration.")
            sys.stdout.flush()
            shutil.copy("config.yaml", "./config/config.yaml")
            try:
                with open("./config/config.yaml", 'r') as stream:
                    conf=yaml.load(stream)
            except yaml.YAMLError as exc:
                print ("Error processing configuration file")
                print(exc)
                sys.stdout.flush()
        else:
            raise
            
    MQTTIP=conf["MQTT"]["IP"]
    MQTTPort=conf["MQTT"]["Port"]
    MQTTUser=conf["MQTT"]["User"]
    MQTTPassword=conf["MQTT"]["Password"]
    MQTTKeepAlive=conf["MQTT"]["KeepAlive"]
    MQTTRootPath=conf["MQTT"]["rootPath"]

    MOXAIP=conf["Moxa"]["IP"]
    MOXAPort=conf["Moxa"]["Port"]

    ScanRate=conf["Settings"]["ScanRate"]


    print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tFMA1600 MQTT Gateway V 0.0.2")

    device_root=MQTTRootPath
    config=communicationConfig(MOXAIP,MOXAPort)
    FlowMeterDevice=FMA1600(RSOverMoxa,config)

    #FlowMeterDevice=FMA1600(dummy_communication,config)
    username=MQTTUser
    password=MQTTPassword

    client = mqtt.Client(client_id="OMEGAREADER")
    client.username_pw_set(username, password=None)
    client.will_set(device_root+"/Info/Status", payload="Offline", qos=0, retain=True)
    client.on_connect = on_connect
    client.on_message = on_message

    client.message_callback_add(device_root+"/Tare", Tare_callback)
    client.message_callback_add(device_root+"/Disconnect", do_disconnect)


    try:
        client.connect(MQTTIP, MQTTPort, MQTTKeepAlive)
        print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tConnected to MQTT Broker: "+MQTTIP+":",MQTTPort)
    except socket.error as e:
        print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tError while connecting to MQTT broker :: %s" % e)




    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_start()

    while True:
        p, t, mq, P = FlowMeterDevice.poll()
        pack={"Pressure":{"Value":p,"Unit":"bar"}, "Temperature":{"Value":t, "Unit":"°C"},"Flow":{"Value":mq, "Unit": "nlpm"},"Power":{"Value":P, "Unit": "kW"}}
        client.publish(device_root+"/Data/All", json.dumps(pack) )
        client.publish(device_root+"/Data/Pressure", json.dumps(pack["Pressure"]))
        client.publish(device_root+"/Data/Temperature", json.dumps(pack["Temperature"]))
        client.publish(device_root+"/Data/Flow", json.dumps(pack["Flow"]))
        y=client.publish(device_root+"/Data/Power", json.dumps(pack["Power"]))
        if y[0] == 4:
            break
        time.sleep(ScanRate)
        
    client.disconnect()
    client.loop_stop(force=False)
    FlowMeterDevice.stop()
    print (datetime.utcnow().strftime('[%Y-%m-%d %H:%M:%S.%f')[:-3]+"]\tProcess stopped")
    sys.stdout.flush()