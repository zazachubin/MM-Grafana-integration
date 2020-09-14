#!/usr/bin/env python3
''' 
    #################### install pysnmp libraries ######################
    python3 -m pip install pysnmp
    ##################### install MQTT libraries #######################
    python3 -m pip install paho-mqtt
    ############## install terminal table visualization ################
    python3 -m pip install texttable
'''
from pysnmp.entity.rfc3413.oneliner import cmdgen
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt
from texttable import Texttable
from datetime import datetime
import pandas as pd
import threading
import struct
import time
import os
import re

################################ MQTT #################################
MQTT_ADDRESS = '159.93.78.13'      # MQTT server address
#MQTT_ADDRESS = 'localhost'         # MQTT Local address
MQTT_TOPIC = 'HV_Cosmic'           # Topic name
MQTT_port = 1883                   # MQTT port
############################## InfluxDB ###############################
InfluxDB_ADDRESS = '159.93.78.13'  # InfluxDB server address
#InfluxDB_ADDRESS = 'localhost'     # InfluxDB Local address
databaseName = 'MM_Dubna'          # Database name
username = 'MM_Dubna'              # Database user name          
password = 'cd4026'                # Database password
DB_port = 8086                     # Database port
################################# HV ##################################
HVaddress = '192.168.0.250'        # HV IP address
HV_port = 161                      # HV port

payload = {}                       # MQTT data container
ReadNumber = 0                     # Counter
Delay = 0.2                        # Device reading delay


client= mqtt.Client("HV")
client.connect(MQTT_ADDRESS, MQTT_port, keepalive=60)
dbclient = InfluxDBClient(  host=InfluxDB_ADDRESS,
                            port=DB_port,
                            username=username,
                            password=password,
                            database=databaseName)

class HVBoard():
    def readDeviceData(self, boardName):
        cmdGen = cmdgen.CommandGenerator()
        errorIndication, errorStatus, errorIndex, varBindTable = cmdGen.bulkCmd(
        cmdgen.CommunityData('public'),
        cmdgen.UdpTransportTarget((HVaddress, HV_port)),
        0,25,
        '1.3.6.1.4.1.19947.1.3.2.1.7',
        '1.3.6.1.4.1.19947.1.3.2.1.6'
        )

        currdata = []
        for var in varBindTable:
            for sv in var:
                oidsplit = sv[0].getOid().prettyPrint().split('.')
                chanidx = oidsplit[-1]
                oid_noidx = '.'.join(oidsplit[:-1])
                if str(oid_noidx) == '1.3.6.1.4.1.19947.1.3.2.1.7':
                    name = 'outputMeasurementCurrent'
                elif str(oid_noidx) == '1.3.6.1.4.1.19947.1.3.2.1.6':
                    name = 'outputMeasurementSenseVoltage'

                if re.search('opaque',sv[-1].prettyPrintType(),re.IGNORECASE):
                    outval = struct.unpack('>f',sv[-1].asOctets()[3:])[0]
                else:
                    outval = sv[-1]._value
                currdata.append([name, outval, chanidx ])

        df = pd.DataFrame(currdata,columns=['measurementType','measurementValue','channel'])

        measurementValue = df['measurementValue']

        if boardName == "B0":
            voltage = {}
            voltage["Ch0"] = measurementValue[1]
            voltage["Ch1"] = measurementValue[3]
            voltage["Ch2"] = measurementValue[5]
            voltage["Ch3"] = measurementValue[7]
            voltage["Ch4"] = measurementValue[9]
            voltage["Ch5"] = measurementValue[11]
            voltage["Ch6"] = measurementValue[13]
            voltage["Ch7"] = measurementValue[15]
            voltage["Ch8"] = measurementValue[17]
            voltage["Ch9"] = measurementValue[19]
            voltage["Ch10"] = measurementValue[21]
            voltage["Ch11"] = measurementValue[23]
            voltage["Ch12"] = measurementValue[25]
            voltage["Ch13"] = measurementValue[27]
            voltage["Ch14"] = measurementValue[29]
            voltage["Ch15"] = measurementValue[31]
            current = {}
            current["Ch0"] = measurementValue[0]
            current["Ch1"] = measurementValue[2]
            current["Ch2"] = measurementValue[4]
            current["Ch3"] = measurementValue[6]
            current["Ch4"] = measurementValue[8]
            current["Ch5"] = measurementValue[10]
            current["Ch6"] = measurementValue[12]
            current["Ch7"] = measurementValue[14]
            current["Ch8"] = measurementValue[16]
            current["Ch9"] = measurementValue[18]
            current["Ch10"] = measurementValue[20]
            current["Ch11"] = measurementValue[22]
            current["Ch12"] = measurementValue[24]
            current["Ch13"] = measurementValue[26]
            current["Ch14"] = measurementValue[28]
            current["Ch15"] = measurementValue[30]

        elif boardName == "B1":
            voltage = {}
            voltage["Ch0"] = measurementValue[33]
            voltage["Ch1"] = measurementValue[35]
            voltage["Ch2"] = measurementValue[37]
            voltage["Ch3"] = measurementValue[39]
            voltage["Ch4"] = measurementValue[41]
            voltage["Ch5"] = measurementValue[43]
            voltage["Ch6"] = measurementValue[45]
            voltage["Ch7"] = measurementValue[47]
            voltage["Ch8"] = measurementValue[49]
            voltage["Ch9"] = measurementValue[51]
            voltage["Ch10"] = measurementValue[53]
            voltage["Ch11"] = measurementValue[55]
            voltage["Ch12"] = measurementValue[57]
            voltage["Ch13"] = measurementValue[59]
            voltage["Ch14"] = measurementValue[61]
            voltage["Ch15"] = measurementValue[63]
            current = {}
            current["Ch0"] = measurementValue[32]
            current["Ch1"] = measurementValue[34]
            current["Ch2"] = measurementValue[36]
            current["Ch3"] = measurementValue[38]
            current["Ch4"] = measurementValue[40]
            current["Ch5"] = measurementValue[42]
            current["Ch6"] = measurementValue[44]
            current["Ch7"] = measurementValue[46]
            current["Ch8"] = measurementValue[48]
            current["Ch9"] = measurementValue[50]
            current["Ch10"] = measurementValue[52]
            current["Ch11"] = measurementValue[54]
            current["Ch12"] = measurementValue[56]
            current["Ch13"] = measurementValue[58]
            current["Ch14"] = measurementValue[60]
            current["Ch15"] = measurementValue[62]

        DataContainer = {}
        DataContainer.update({"Voltage" : voltage})
        DataContainer.update({"Current" : current})

        return DataContainer

def readHV2mqtt():
    global payload, ReadNumber
    B0 = HVBoard()
    B1 = HVBoard()

    while True:
        payload = {}
        payload.update({"B0" : B0.readDeviceData('B0')})
        payload.update({"B1" : B1.readDeviceData('B1')})

        client.publish(MQTT_TOPIC, str(payload))
        ReadNumber +=1
        time.sleep(Delay)

def printDataTable():
    while True:
        try:
            ####################################### Table Viszualization #########################################
            tableData = [['Channels', 'B0_Voltage [V]', 'B0_Currents [A]', 'B1_Voltage [V]', 'B1_Currents [A]']]

            tableData.append([ "Ch0", str(round(payload["B0"]["Voltage"]["Ch0"],2)) + " V", str(round(payload["B0"]["Current"]["Ch0"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch0"],2)) + " V", str(round(payload["B1"]["Current"]["Ch0"],11))+" A" ])
            tableData.append([ "Ch1", str(round(payload["B0"]["Voltage"]["Ch1"],2)) + " V", str(round(payload["B0"]["Current"]["Ch1"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch1"],2)) + " V", str(round(payload["B1"]["Current"]["Ch1"],11))+" A" ])
            tableData.append([ "Ch2", str(round(payload["B0"]["Voltage"]["Ch2"],2)) + " V", str(round(payload["B0"]["Current"]["Ch2"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch2"],2)) + " V", str(round(payload["B1"]["Current"]["Ch2"],11))+" A" ])
            tableData.append([ "Ch3", str(round(payload["B0"]["Voltage"]["Ch3"],2)) + " V", str(round(payload["B0"]["Current"]["Ch3"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch3"],2)) + " V", str(round(payload["B1"]["Current"]["Ch3"],11))+" A" ])
            tableData.append([ "Ch4", str(round(payload["B0"]["Voltage"]["Ch4"],2)) + " V", str(round(payload["B0"]["Current"]["Ch4"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch4"],2)) + " V", str(round(payload["B1"]["Current"]["Ch4"],11))+" A" ])
            tableData.append([ "Ch5", str(round(payload["B0"]["Voltage"]["Ch5"],2)) + " V", str(round(payload["B0"]["Current"]["Ch5"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch5"],2)) + " V", str(round(payload["B1"]["Current"]["Ch5"],11))+" A" ])
            tableData.append([ "Ch6", str(round(payload["B0"]["Voltage"]["Ch6"],2)) + " V", str(round(payload["B0"]["Current"]["Ch6"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch6"],2)) + " V", str(round(payload["B1"]["Current"]["Ch6"],11))+" A" ])
            tableData.append([ "Ch7", str(round(payload["B0"]["Voltage"]["Ch7"],2)) + " V", str(round(payload["B0"]["Current"]["Ch7"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch7"],2)) + " V", str(round(payload["B1"]["Current"]["Ch7"],11))+" A" ])
            tableData.append([ "Ch8", str(round(payload["B0"]["Voltage"]["Ch8"],2)) + " V", str(round(payload["B0"]["Current"]["Ch8"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch8"],2)) + " V", str(round(payload["B1"]["Current"]["Ch8"],11))+" A" ])
            tableData.append([ "Ch9", str(round(payload["B0"]["Voltage"]["Ch9"],2)) + " V", str(round(payload["B0"]["Current"]["Ch9"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch9"],2)) + " V", str(round(payload["B1"]["Current"]["Ch9"],11))+" A" ])
            tableData.append([ "Ch10", str(round(payload["B0"]["Voltage"]["Ch10"],2)) + " V", str(round(payload["B0"]["Current"]["Ch10"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch10"],2)) + " V", str(round(payload["B1"]["Current"]["Ch10"],11))+" A" ])
            tableData.append([ "Ch11", str(round(payload["B0"]["Voltage"]["Ch11"],2)) + " V", str(round(payload["B0"]["Current"]["Ch11"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch11"],2)) + " V", str(round(payload["B1"]["Current"]["Ch11"],11))+" A" ])
            tableData.append([ "Ch12", str(round(payload["B0"]["Voltage"]["Ch12"],2)) + " V", str(round(payload["B0"]["Current"]["Ch12"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch12"],2)) + " V", str(round(payload["B1"]["Current"]["Ch12"],11))+" A" ])
            tableData.append([ "Ch13", str(round(payload["B0"]["Voltage"]["Ch13"],2)) + " V", str(round(payload["B0"]["Current"]["Ch13"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch13"],2)) + " V", str(round(payload["B1"]["Current"]["Ch13"],11))+" A" ])
            tableData.append([ "Ch14", str(round(payload["B0"]["Voltage"]["Ch14"],2)) + " V", str(round(payload["B0"]["Current"]["Ch14"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch14"],2)) + " V", str(round(payload["B1"]["Current"]["Ch14"],11))+" A" ])
            tableData.append([ "Ch15", str(round(payload["B0"]["Voltage"]["Ch15"],2)) + " V", str(round(payload["B0"]["Current"]["Ch15"],11))+" A" , str(round(payload["B1"]["Voltage"]["Ch15"],2)) + " V", str(round(payload["B1"]["Current"]["Ch15"],11))+" A" ])

            Table = Texttable()
            Table.add_rows(tableData)
            TableText = Table.draw()
            print(TableText)
            print("Read number : {} ".format(ReadNumber))
            CurrentTime = datetime.now()
            print("date and time = ", CurrentTime)
            #print("Read delay = {}".format(Delay))
            time.sleep(0.5)
            os.system('clear')
        except KeyError:
            pass

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

    influxdbLineCommand.append("{measurement},Voltage={Voltage} L8={L8},R8={R8},L7={L7},R7={R7},L6={L6},R6={R6} {timestamp}"
                                .format(measurement='HV_COSMIC_STAND',
                                        Voltage='L1',
                                        L8=data['B1']['Voltage']['Ch7'],
                                        R8=data['B1']['Voltage']['Ch6'],
                                        L7=data['B1']['Voltage']['Ch15'],
                                        R7=data['B1']['Voltage']['Ch14'],
                                        L6=data['B0']['Voltage']['Ch7'],
                                        R6=data['B0']['Voltage']['Ch6'],
                                        timestamp=data_end_time))

    influxdbLineCommand.append("{measurement},Current={Current} L8={L8},R8={R8},L7={L7},R7={R7},L6={L6},R6={R6} {timestamp}"
                                .format(measurement='HV_COSMIC_STAND',
                                        Current='L1',
                                        L8=data['B1']['Current']['Ch7'],
                                        R8=data['B1']['Current']['Ch6'],
                                        L7=data['B1']['Current']['Ch15'],
                                        R7=data['B1']['Current']['Ch14'],
                                        L6=data['B0']['Current']['Ch7'],
                                        R6=data['B0']['Current']['Ch6'],
                                        timestamp=data_end_time))

    influxdbLineCommand.append("{measurement},Voltage={Voltage} L8={L8},R8={R8},L7={L7},R7={R7},L6={L6},R6={R6} {timestamp}"
                                .format(measurement='HV_COSMIC_STAND',
                                        Voltage='L2',
                                        L8=data['B1']['Voltage']['Ch5'],
                                        R8=data['B1']['Voltage']['Ch4'],
                                        L7=data['B1']['Voltage']['Ch13'],
                                        R7=data['B1']['Voltage']['Ch12'],
                                        L6=data['B0']['Voltage']['Ch5'],
                                        R6=data['B0']['Voltage']['Ch4'],
                                        timestamp=data_end_time))

    influxdbLineCommand.append("{measurement},Current={Current} L8={L8},R8={R8},L7={L7},R7={R7},L6={L6},R6={R6} {timestamp}"
                                .format(measurement='HV_COSMIC_STAND',
                                        Current='L2',
                                        L8=data['B1']['Current']['Ch5'],
                                        R8=data['B1']['Current']['Ch4'],
                                        L7=data['B1']['Current']['Ch13'],
                                        R7=data['B1']['Current']['Ch12'],
                                        L6=data['B0']['Current']['Ch5'],
                                        R6=data['B0']['Current']['Ch4'],
                                        timestamp=data_end_time))

    influxdbLineCommand.append("{measurement},Voltage={Voltage} L8={L8},R8={R8},L7={L7},R7={R7},L6={L6},R6={R6} {timestamp}"
                                .format(measurement='HV_COSMIC_STAND',
                                        Voltage='L3',
                                        L8=data['B1']['Voltage']['Ch3'],
                                        R8=data['B1']['Voltage']['Ch2'],
                                        L7=data['B1']['Voltage']['Ch11'],
                                        R7=data['B1']['Voltage']['Ch10'],
                                        L6=data['B0']['Voltage']['Ch3'],
                                        R6=data['B0']['Voltage']['Ch2'],
                                        timestamp=data_end_time))

    influxdbLineCommand.append("{measurement},Current={Current} L8={L8},R8={R8},L7={L7},R7={R7},L6={L6},R6={R6} {timestamp}"
                                .format(measurement='HV_COSMIC_STAND',
                                        Current='L3',
                                        L8=data['B1']['Current']['Ch3'],
                                        R8=data['B1']['Current']['Ch2'],
                                        L7=data['B1']['Current']['Ch11'],
                                        R7=data['B1']['Current']['Ch10'],
                                        L6=data['B0']['Current']['Ch3'],
                                        R6=data['B0']['Current']['Ch2'],
                                        timestamp=data_end_time))

    influxdbLineCommand.append("{measurement},Voltage={Voltage} L8={L8},R8={R8},L7={L7},R7={R7},L6={L6},R6={R6} {timestamp}"
                                .format(measurement='HV_COSMIC_STAND',
                                        Voltage='L4',
                                        L8=data['B1']['Voltage']['Ch1'],
                                        R8=data['B1']['Voltage']['Ch0'],
                                        L7=data['B1']['Voltage']['Ch9'],
                                        R7=data['B1']['Voltage']['Ch8'],
                                        L6=data['B0']['Voltage']['Ch1'],
                                        R6=data['B0']['Voltage']['Ch0'],
                                        timestamp=data_end_time))

    influxdbLineCommand.append("{measurement},Current={Current} L8={L8},R8={R8},L7={L7},R7={R7},L6={L6},R6={R6} {timestamp}"
                                .format(measurement='HV_COSMIC_STAND',
                                        Current='L4',
                                        L8=data['B1']['Current']['Ch1'],
                                        R8=data['B1']['Current']['Ch0'],
                                        L7=data['B1']['Current']['Ch9'],
                                        R7=data['B1']['Current']['Ch8'],
                                        L6=data['B0']['Current']['Ch1'],
                                        R6=data['B0']['Current']['Ch0'],
                                        timestamp=data_end_time))

    if {'name' : databaseName} in dbclient.get_list_database():
        dbclient.write_points(influxdbLineCommand, database=databaseName, time_precision='ms', protocol='line')
        print("UTC-Time: {} ---> Point : {} ---> Finished writing to InfluxDB".format(CurrentTime, ReadNumber), end='\r')
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
    my_thread1 = threading.Thread(target=readHV2mqtt)
    my_thread2 = threading.Thread(target=subscriber2influxdb)
    #my_thread3 = threading.Thread(target=printDataTable)
    my_thread1.start()
    my_thread2.start()
    #my_thread3.start()