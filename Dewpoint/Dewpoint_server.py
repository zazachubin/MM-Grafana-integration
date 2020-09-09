#!/usr/bin/env python3

''' 
    ################### install picolog libraries ######################
    #Add repository to the updater
    sudo bash -c 'echo "deb https://labs.picotech.com/debian/ picoscope main" >/etc/apt/sources.list.d/picoscope.list'

    #Import public key
    wget -O - https://labs.picotech.com/debian/dists/picoscope/Release.gpg.key | sudo apt-key add -

    #Update package manager cache
    sudo apt-get update

    #Install PicoScope
    sudo apt-get install picoscope

    #################### install picolog wrappers ######################
    python3 -m pip install git+https://github.com/picotech/picosdk-python-wrappers
    ##################### install MQTT libraries #######################
    python3 -m pip install paho-mqtt
    ############## install terminal table visualization ################
    python3 -m pip install texttable
'''

from picosdk.functions import assert_pico_ok
from picosdk.pl1000 import pl1000 as pl
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt
from texttable import Texttable
from datetime import datetime
import pandas as pd
import threading
import ctypes
import struct
import time
import os
import re

################################ MQTT #################################
MQTT_ADDRESS = '159.93.78.13'      # MQTT server address
#MQTT_ADDRESS = 'localhost'         # MQTT Local address
MQTT_TOPIC = 'Dewpoint'           # Topic name
MQTT_port = 1883                   # MQTT port
############################## InfluxDB ###############################
InfluxDB_ADDRESS = '159.93.78.13'  # InfluxDB server address
#InfluxDB_ADDRESS = 'localhost'     # InfluxDB Local address
databaseName = 'MM_Dubna'          # Database name
username = 'MM_Dubna'              # Database user name          
password = 'cd4026'                # Database password
DB_port = 8086                     # Database port

payload = {}                       # MQTT data container
ReadNumber = 0                     # Counter
Delay = 1                          # Device reading delay


chandle = ctypes.c_int16()
status = {}
# open PicoLog 1000 device
status["openUnit"] = pl.pl1000OpenUnit(ctypes.byref(chandle))
assert_pico_ok(status["openUnit"])
value = ctypes.c_int16()


client= mqtt.Client("Dewpoint")
client.connect(MQTT_ADDRESS, MQTT_port, keepalive=60)
dbclient = InfluxDBClient(  host=InfluxDB_ADDRESS,
                            port=DB_port,
                            username=username,
                            password=password,
                            database=databaseName)

def readDeviceData():
    # get adc value from picolg
    status["getSingle"] = pl.pl1000GetSingle(chandle, pl.PL1000Inputs["PL1000_CHANNEL_1"], ctypes.byref(value))
    voltage = value.value * 2.5 / 4095
    dewpoint = (voltage-2.016) / 0.0192
    humidity = 2983.6* ( 2.7183** (-0.5 * (((voltage-2.016) / 0.0192 - 248) / 141.2)**4))

    Humidity = {}
    Humidity["Dewpoint"] = round(dewpoint,2)
    Humidity["Humidity"] = round(humidity,2)
    return Humidity

def readDewpoint2mqtt():
    global ReadNumber
    while True:
        dewpoint = readDeviceData()
        client.publish(MQTT_TOPIC, str(dewpoint))
        '''
        ####################################### Table Viszualization #########################################
        tableData = [['Time', 'Diwpoint [C]', 'Humidity [%]', 'Points']]
        CurrentTime = datetime.now()
        tableData.append([ str(CurrentTime), str(dewpoint["Dewpoint"]), str(dewpoint["Humidity"]), str(ReadNumber) ])
        
        Table = Texttable()
        Table.add_rows(tableData)
        TableText = Table.draw()
        print(TableText)
        os.system('clear')
        '''
        ReadNumber +=1
        time.sleep(Delay)

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    global dbclient
    data = eval(str(msg.payload.decode('utf-8')))

    influxdbLineCommand = []
   #################### Time in miliseconds UTC ##########################
    CurrentTime = datetime.utcnow().timestamp()
    data_end_time = int(CurrentTime * 1000)
    CurrentTime = datetime.fromtimestamp(CurrentTime)

    influxdbLineCommand.append("{measurement},Sensor={Sensor} Dewpoint={Dewpoint} {timestamp}"
                                .format(measurement='Dewpoint_COSMIC_STAND',
                                        Sensor='DMT143',
                                        Dewpoint=data['Dewpoint'],
                                        timestamp=data_end_time))
    
    influxdbLineCommand.append("{measurement},Sensor={Sensor} Humidity={Humidity} {timestamp}"
                                .format(measurement='Dewpoint_COSMIC_STAND',
                                        Sensor='DMT143',
                                        Humidity=data['Humidity'],
                                        timestamp=data_end_time))

    if {'name' : databaseName} in dbclient.get_list_database():
        dbclient.write_points(influxdbLineCommand, database=databaseName, time_precision='ms', protocol='line')
        print("UTC-Time: {} --> Dewpoint: {} --> Point : {} --> Finished writing to InfluxDB".format(CurrentTime, data['Dewpoint'], ReadNumber), end='\r')
        pass

    else:
        dbclient.create_database(databaseName)
        print("Creating database ...")
        dbclient.write_points(influxdbLineCommand, database=databaseName, time_precision='ms', protocol='line')
        print("Finished writing to InfluxDB")

def subscriber2influxdb():
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, MQTT_port)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    my_thread1 = threading.Thread(target=readDewpoint2mqtt)
    my_thread2 = threading.Thread(target=subscriber2influxdb)
    my_thread1.start()
    my_thread2.start()