#!/usr/bin/env python3
''' Source Code: https://github.com/zazachubin/MM-Grafana-integration
    #################### install pysnmp libraries ######################
    python3 -m pip install pysnmp
    ##################### install MQTT libraries #######################
    python3 -m pip install paho-mqtt
    ############## install terminal table visualization ################
    python3 -m pip install texttable
    ######################### install InfluxDb #########################
    # python3 -m pip install influxdb
    # python3 -m pip install influxdb-client

    test run: python3 HV_server_test.py --M <ModuleName>
'''
from pysnmp.entity.rfc3413.oneliner import cmdgen
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt
from texttable import Texttable
from datetime import datetime
import pandas as pd
import threading
import argparse
import struct
import time
import os
import re

################################ MQTT #################################
MQTT_ADDRESS = 'localhost'         # MQTT Local address
MQTT_TOPIC = 'HV_Cosmic'           # Topic name
MQTT_port = 1883                   # MQTT port
############################## InfluxDB ###############################
InfluxDB_ADDRESS = 'bes3.jinr.ru'  # InfluxDB Local address
databaseName = 'MM_Dubna'          # Database name
username = 'MM_Dubna'              # Database user name          
password = '********'              # Database password
DB_port = 8086                     # Database port
################################# HV ##################################
HVaddress = '192.168.0.250'        # HV IP address
HV_port = 161                      # HV port

payload = {}                       # MQTT data container
ReadNumber = 0                     # Counter
Delay = 0.2                        # Device reading delay
ModuleName = ''                    # Measuring module name

client= mqtt.Client("HV")
client.connect(MQTT_ADDRESS, MQTT_port, keepalive=60)
dbclient = InfluxDBClient(  host=InfluxDB_ADDRESS,
                            port=DB_port,
                            username=username,
                            password=password,
                            database=databaseName)

startTimeStamp = datetime.fromtimestamp(datetime.utcnow().timestamp())
lognameB0 = "HV_B0_{}.csv".format(str(startTimeStamp))
lognameB1 = "HV_B1_{}.csv".format(str(startTimeStamp))

f1 = open(lognameB0, "a")
f2 = open(lognameB1, "a")
f1.write("{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{}\n".format(
'Time',
'V_ch0',
'I_ch0',
'V_ch1',
'I_ch1',
'V_ch2',
'I_ch2',
'V_ch3',
'I_ch3',
'V_ch4',
'I_ch4',
'V_ch5',
'I_ch5',
'V_ch6',
'I_ch6',
'V_ch7',
'I_ch7',
'V_ch8',
'I_ch8',
'V_ch9',
'I_ch9',
'V_ch10',
'I_ch10',
'V_ch11',
'I_ch11',
'V_ch12',
'I_ch12',
'V_ch13',
'I_ch13',
'V_ch14',
'I_ch14',
'V_ch15',
'I_ch15',))

f2.write("{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{}\n".format(
'Time',
'V_ch0',
'I_ch0',
'V_ch1',
'I_ch1',
'V_ch2',
'I_ch2',
'V_ch3',
'I_ch3',
'V_ch4',
'I_ch4',
'V_ch5',
'I_ch5',
'V_ch6',
'I_ch6',
'V_ch7',
'I_ch7',
'V_ch8',
'I_ch8',
'V_ch9',
'I_ch9',
'V_ch10',
'I_ch10',
'V_ch11',
'I_ch11',
'V_ch12',
'I_ch12',
'V_ch13',
'I_ch13',
'V_ch14',
'I_ch14',
'V_ch15',
'I_ch15',))

f1.close()
f2.close()

class HVBoard():
    def readDeviceData(self, boardName):
        cmdGen = cmdgen.CommandGenerator()
        errorIndication, errorStatus, errorIndex, varBindTable = cmdGen.bulkCmd(
        cmdgen.CommunityData('public'),
        cmdgen.UdpTransportTarget((HVaddress, HV_port)),
        0,25,
        '1.3.6.1.4.1.19947.1.3.2.1.7', # outputMeasurementCurrent
        '1.3.6.1.4.1.19947.1.3.2.1.6'  # outputMeasurementSenseVoltage
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

def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        description='HV server logger')
    parser.add_argument('--M', type=str, required=False,
                        default='M_test',
                        help='Module name')
    return parser.parse_args()

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

    influxdbContainer = []
    ############### Time in UTC ################
    CurrentTime = datetime.fromtimestamp(datetime.utcnow().timestamp())

    f1 = open(lognameB0, "a")
    f2 = open(lognameB1, "a")

    f1.write("{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{}\n".format(
        CurrentTime,
        data['B0']['Voltage']['Ch0'],
        data['B0']['Current']['Ch0'],
        data['B0']['Voltage']['Ch1'],
        data['B0']['Current']['Ch1'],
        data['B0']['Voltage']['Ch2'],
        data['B0']['Current']['Ch2'],
        data['B0']['Voltage']['Ch3'],
        data['B0']['Current']['Ch3'],
        data['B0']['Voltage']['Ch4'],
        data['B0']['Current']['Ch4'],
        data['B0']['Voltage']['Ch5'],
        data['B0']['Current']['Ch5'],
        data['B0']['Voltage']['Ch6'],
        data['B0']['Current']['Ch6'],
        data['B0']['Voltage']['Ch7'],
        data['B0']['Current']['Ch7'],
        data['B0']['Voltage']['Ch8'],
        data['B0']['Current']['Ch8'],
        data['B0']['Voltage']['Ch9'],
        data['B0']['Current']['Ch9'],
        data['B0']['Voltage']['Ch10'],
        data['B0']['Current']['Ch10'],
        data['B0']['Voltage']['Ch11'],
        data['B0']['Current']['Ch11'],
        data['B0']['Voltage']['Ch12'],
        data['B0']['Current']['Ch12'],
        data['B0']['Voltage']['Ch13'],
        data['B0']['Current']['Ch13'],
        data['B0']['Voltage']['Ch14'],
        data['B0']['Current']['Ch14'],
        data['B0']['Voltage']['Ch15'],
        data['B0']['Current']['Ch15']))
    
    f2.write("{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{}\n".format(
        CurrentTime,
        data['B1']['Voltage']['Ch0'],
        data['B1']['Current']['Ch0'],
        data['B1']['Voltage']['Ch1'],
        data['B1']['Current']['Ch1'],
        data['B1']['Voltage']['Ch2'],
        data['B1']['Current']['Ch2'],
        data['B1']['Voltage']['Ch3'],
        data['B1']['Current']['Ch3'],
        data['B1']['Voltage']['Ch4'],
        data['B1']['Current']['Ch4'],
        data['B1']['Voltage']['Ch5'],
        data['B1']['Current']['Ch5'],
        data['B1']['Voltage']['Ch6'],
        data['B1']['Current']['Ch6'],
        data['B1']['Voltage']['Ch7'],
        data['B1']['Current']['Ch7'],
        data['B1']['Voltage']['Ch8'],
        data['B1']['Current']['Ch8'],
        data['B1']['Voltage']['Ch9'],
        data['B1']['Current']['Ch9'],
        data['B1']['Voltage']['Ch10'],
        data['B1']['Current']['Ch10'],
        data['B1']['Voltage']['Ch11'],
        data['B1']['Current']['Ch11'],
        data['B1']['Voltage']['Ch12'],
        data['B1']['Current']['Ch12'],
        data['B1']['Voltage']['Ch13'],
        data['B1']['Current']['Ch13'],
        data['B1']['Voltage']['Ch14'],
        data['B1']['Current']['Ch14'],
        data['B1']['Voltage']['Ch15'],
        data['B1']['Current']['Ch15']))
    f1.close()
    f2.close()

    ################### L1 #####################
    ## PCB L8
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L1",
                "PCB" : "L8"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch7'],
                "Current" : data['B1']['Current']['Ch7']
            }
        }
    )
    ## PCB R8
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L1",
                "PCB" : "R8"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch6'],
                "Current" : data['B1']['Current']['Ch6']
            }
        }
    )
    ## PCB L7
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L1",
                "PCB" : "L7"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch15'],
                "Current" : data['B1']['Current']['Ch15']
            }
        }
    )
    ## PCB R7
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L1",
                "PCB" : "R7"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch14'],
                "Current" : data['B1']['Current']['Ch14']
            }
        }
    )
    ## PCB L6
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L1",
                "PCB" : "L6"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B0']['Voltage']['Ch7'],
                "Current" : data['B0']['Current']['Ch7']
            }
        }
    )
    ## PCB R6
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L1",
                "PCB" : "R6"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B0']['Voltage']['Ch6'],
                "Current" : data['B0']['Current']['Ch6']
            }
        }
    )
    ################### L2 #####################
    ## PCB L8
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L2",
                "PCB" : "L8"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch5'],
                "Current" : data['B1']['Current']['Ch5']
            }
        }
    )
    ## PCB R8
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L2",
                "PCB" : "R8"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch4'],
                "Current" : data['B1']['Current']['Ch4']
            }
        }
    )
    ## PCB L7
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L2",
                "PCB" : "L7"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch13'],
                "Current" : data['B1']['Current']['Ch13']
            }
        }
    )
    ## PCB R7
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L2",
                "PCB" : "R7"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch12'],
                "Current" : data['B1']['Current']['Ch12']
            }
        }
    )
    ## PCB L6
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L2",
                "PCB" : "L6"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B0']['Voltage']['Ch5'],
                "Current" : data['B0']['Current']['Ch5']
            }
        }
    )
    ## PCB R6
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L2",
                "PCB" : "R6"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B0']['Voltage']['Ch4'],
                "Current" : data['B0']['Current']['Ch4']
            }
        }
    )
    ################### L3 #####################
    ## PCB L8
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L3",
                "PCB" : "L8"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch3'],
                "Current" : data['B1']['Current']['Ch3']
            }
        }
    )
    ## PCB R8
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L3",
                "PCB" : "R8"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch2'],
                "Current" : data['B1']['Current']['Ch2']
            }
        }
    )
    ## PCB L7
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L3",
                "PCB" : "L7"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch11'],
                "Current" : data['B1']['Current']['Ch11']
            }
        }
    )
    ## PCB R7
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L3",
                "PCB" : "R7"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch10'],
                "Current" : data['B1']['Current']['Ch10']
            }
        }
    )
    ## PCB L6
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L3",
                "PCB" : "L6"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B0']['Voltage']['Ch3'],
                "Current" : data['B0']['Current']['Ch3']
            }
        }
    )
    ## PCB R6
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L3",
                "PCB" : "R6"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B0']['Voltage']['Ch2'],
                "Current" : data['B0']['Current']['Ch2']
            }
        }
    )
    ################### L4 #####################
    ## PCB L8
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L4",
                "PCB" : "L8"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch1'],
                "Current" : data['B1']['Current']['Ch1']
            }
        }
    )
    ## PCB R8
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L4",
                "PCB" : "R8"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch0'],
                "Current" : data['B1']['Current']['Ch0']
            }
        }
    )
    ## PCB L7
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L4",
                "PCB" : "L7"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch9'],
                "Current" : data['B1']['Current']['Ch9']
            }
        }
    )
    ## PCB R7
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L4",
                "PCB" : "R7"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B1']['Voltage']['Ch8'],
                "Current" : data['B1']['Current']['Ch8']
            }
        }
    )
    ## PCB L6
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L4",
                "PCB" : "L6"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B0']['Voltage']['Ch1'],
                "Current" : data['B0']['Current']['Ch1']
            }
        }
    )
    ## PCB R6
    influxdbContainer.append(
        {
            "measurement": "HV_COSMIC_STAND",
            "tags": {
                "Module" : ModuleName,
                "Layer" : "L4",
                "PCB" : "R6"
            },
            "time": CurrentTime,
            "fields": {
                "Voltage" : data['B0']['Voltage']['Ch0'],
                "Current" : data['B0']['Current']['Ch0']
            }
        }
    )

    if {'name' : databaseName} in dbclient.get_list_database():
        dbclient.write_points(influxdbContainer, database=databaseName)
        print("UTC-Time: {} ---> Module: {} ---> Point : {} ---> Finished writing to InfluxDB".format(CurrentTime, ModuleName, ReadNumber), end='\r')
        pass

    else:
        dbclient.create_database(databaseName)
        print("Creating database ...")
        dbclient.write_points(influxdbContainer, database=databaseName)
        print("Finished writing to InfluxDB")

def subscriber2influxdb(args):
    global ModuleName
    ModuleName = args.M
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, MQTT_port)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    args = parse_args()
    my_thread1 = threading.Thread(target=readHV2mqtt)
    my_thread2 = threading.Thread(target=subscriber2influxdb, args=(args,))
    #my_thread3 = threading.Thread(target=printDataTable)
    my_thread1.start()
    my_thread2.start()
    #my_thread3.start()