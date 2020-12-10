#!/usr/bin/env python3
''' Source Code: https://github.com/zazachubin/MM-Grafana-integration
    #################### install pysnmp libraries ######################
    python3 -m pip install pysnmp
    ############## install terminal table visualization ################
    python3 -m pip install texttable
    ######################### install InfluxDb #########################
    # python3 -m pip install influxdb
    # python3 -m pip install influxdb-client

    Run script: python3 HV_server.py --M <ModuleName>
'''
from pysnmp.entity.rfc3413.oneliner import cmdgen
from influxdb import InfluxDBClient
from texttable import Texttable
from datetime import datetime, timedelta
import pandas as pd
import threading
import argparse
import struct
import time
import os
import re

############################## InfluxDB ###############################
InfluxDB_ADDRESS = 'bes3.jinr.ru'  # InfluxDB Local address
databaseName = 'MM_Dubna'          # Database name
username = 'MM_Dubna'              # Database user name          
password = '********'              # Database password
DB_port = 8086                     # Database port
################################# HV ##################################
HVaddress = '192.168.0.250'        # HV IP address
HV_port = 161                      # HV port
############################# Variables ###############################
ReadNumber = 1                     # Counter
Delay = 0.05                       # Device reading delay
ModuleName = ""                    # Measuring module name
databaseStatus = "on"              # Database on/off status
TermPrint = "off"                  # Print data in terminal
logstatus = "on"                   # Local log on/off
lognameB0 = ""                     # Board 0 data log name
lognameB1 = ""                     # Board 1 data log name
databaseStatus = "on"              # Database on/off status
avgTime = 1.0                      # Averaging time
deltaTime = 0                      # Data collecting time counter
timeStampContainer = []            # TimeStamp buffer
hv_DataContainer = []              # HV data buffer
influxdbContainer = []             # Database container
hv_Data_Avg = {}                   # HV avg data container
#######################################################################
def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
                        description='HV server logger')
    parser.add_argument('--M', type=str, required=False,
                        default='M_test',
                        help='Module name')
    parser.add_argument('--delay', type=float, required=False,
                        default=Delay,
                        help='HV device reading delay')
    parser.add_argument('--TermPrint', type=str, required=False,
                        default=TermPrint,
                        help='Terminal print data on/off')
    parser.add_argument('--log', type=str, required=False,
                        default=logstatus,
                        help='log on/off')
    parser.add_argument('--database', type=str, required=False,
                        default=databaseStatus,
                        help='database on/off')
    parser.add_argument('--hvIp', type=str, required=False,
                        default=HVaddress,
                        help='HV IP Address')
    parser.add_argument('--avgTime', type=float, required=False,
                        default=avgTime,
                        help='Averaging time')
    parser.add_argument('--dbHost', type=str, required=False,
                        default=InfluxDB_ADDRESS,
                        help='Database host address')
    parser.add_argument('--dbName', type=str, required=False,
                        default=databaseName,
                        help='Database name')
    parser.add_argument('--dbUserName', type=str, required=False,
                        default=username,
                        help='Database user name')
    parser.add_argument('--dbPasswd', type=str, required=False,
                        default=password,
                        help='Database password')
    parser.add_argument('--dbPort', type=int, required=False,
                        default=DB_port,
                        help='Database port number')
    return parser.parse_args()

args = parse_args()

ModuleName = args.M
Delay = args.delay
TermPrint = args.TermPrint
logstatus = args.log
databaseStatus = args.database
HVaddress = args.hvIp
avgTime = args.avgTime

InfluxDB_ADDRESS = args.dbHost
DB_port = args.dbPort
databaseName = args.dbName
username = args.dbUserName
password = args.dbPasswd

if databaseStatus == "on":
    dbclient = InfluxDBClient(  host=InfluxDB_ADDRESS,
                                port=DB_port,
                                username=username,
                                password=password,
                                database=databaseName)

class HVBoard():
    def readDeviceData(self):
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

        voltageB0 = {}
        voltageB0["Ch0"] = measurementValue[1]
        voltageB0["Ch1"] = measurementValue[3]
        voltageB0["Ch2"] = measurementValue[5]
        voltageB0["Ch3"] = measurementValue[7]
        voltageB0["Ch4"] = measurementValue[9]
        voltageB0["Ch5"] = measurementValue[11]
        voltageB0["Ch6"] = measurementValue[13]
        voltageB0["Ch7"] = measurementValue[15]
        voltageB0["Ch8"] = measurementValue[17]
        voltageB0["Ch9"] = measurementValue[19]
        voltageB0["Ch10"] = measurementValue[21]
        voltageB0["Ch11"] = measurementValue[23]
        voltageB0["Ch12"] = measurementValue[25]
        voltageB0["Ch13"] = measurementValue[27]
        voltageB0["Ch14"] = measurementValue[29]
        voltageB0["Ch15"] = measurementValue[31]
        currentB0 = {}
        currentB0["Ch0"] = measurementValue[0]
        currentB0["Ch1"] = measurementValue[2]
        currentB0["Ch2"] = measurementValue[4]
        currentB0["Ch3"] = measurementValue[6]
        currentB0["Ch4"] = measurementValue[8]
        currentB0["Ch5"] = measurementValue[10]
        currentB0["Ch6"] = measurementValue[12]
        currentB0["Ch7"] = measurementValue[14]
        currentB0["Ch8"] = measurementValue[16]
        currentB0["Ch9"] = measurementValue[18]
        currentB0["Ch10"] = measurementValue[20]
        currentB0["Ch11"] = measurementValue[22]
        currentB0["Ch12"] = measurementValue[24]
        currentB0["Ch13"] = measurementValue[26]
        currentB0["Ch14"] = measurementValue[28]
        currentB0["Ch15"] = measurementValue[30]

        voltageB1 = {}
        voltageB1["Ch0"] = measurementValue[33]
        voltageB1["Ch1"] = measurementValue[35]
        voltageB1["Ch2"] = measurementValue[37]
        voltageB1["Ch3"] = measurementValue[39]
        voltageB1["Ch4"] = measurementValue[41]
        voltageB1["Ch5"] = measurementValue[43]
        voltageB1["Ch6"] = measurementValue[45]
        voltageB1["Ch7"] = measurementValue[47]
        voltageB1["Ch8"] = measurementValue[49]
        voltageB1["Ch9"] = measurementValue[51]
        voltageB1["Ch10"] = measurementValue[53]
        voltageB1["Ch11"] = measurementValue[55]
        voltageB1["Ch12"] = measurementValue[57]
        voltageB1["Ch13"] = measurementValue[59]
        voltageB1["Ch14"] = measurementValue[61]
        voltageB1["Ch15"] = measurementValue[63]
        currentB1 = {}
        currentB1["Ch0"] = measurementValue[32]
        currentB1["Ch1"] = measurementValue[34]
        currentB1["Ch2"] = measurementValue[36]
        currentB1["Ch3"] = measurementValue[38]
        currentB1["Ch4"] = measurementValue[40]
        currentB1["Ch5"] = measurementValue[42]
        currentB1["Ch6"] = measurementValue[44]
        currentB1["Ch7"] = measurementValue[46]
        currentB1["Ch8"] = measurementValue[48]
        currentB1["Ch9"] = measurementValue[50]
        currentB1["Ch10"] = measurementValue[52]
        currentB1["Ch11"] = measurementValue[54]
        currentB1["Ch12"] = measurementValue[56]
        currentB1["Ch13"] = measurementValue[58]
        currentB1["Ch14"] = measurementValue[60]
        currentB1["Ch15"] = measurementValue[62]

        DataContainer = {}
        DataContainer.update({"VoltageB0" : voltageB0})
        DataContainer.update({"CurrentB0" : currentB0})
        DataContainer.update({"VoltageB1" : voltageB1})
        DataContainer.update({"CurrentB1" : currentB1})

        return DataContainer

def printDataTable():
    global hv_Data_Avg
    while True:
        try:
            ####################################### Table Viszualization #########################################
            tableData = [['Channels', 'B0_Voltage [V]', 'B0_Currents [A]', 'B1_Voltage [V]', 'B1_Currents [A]']]

            tableData.append([ "Ch0", str(round(hv_Data_Avg["VoltageB0"]["Ch0"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch0"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch0"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch0"],11))+" A" ])
            tableData.append([ "Ch1", str(round(hv_Data_Avg["VoltageB0"]["Ch1"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch1"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch1"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch1"],11))+" A" ])
            tableData.append([ "Ch2", str(round(hv_Data_Avg["VoltageB0"]["Ch2"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch2"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch2"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch2"],11))+" A" ])
            tableData.append([ "Ch3", str(round(hv_Data_Avg["VoltageB0"]["Ch3"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch3"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch3"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch3"],11))+" A" ])
            tableData.append([ "Ch4", str(round(hv_Data_Avg["VoltageB0"]["Ch4"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch4"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch4"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch4"],11))+" A" ])
            tableData.append([ "Ch5", str(round(hv_Data_Avg["VoltageB0"]["Ch5"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch5"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch5"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch5"],11))+" A" ])
            tableData.append([ "Ch6", str(round(hv_Data_Avg["VoltageB0"]["Ch6"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch6"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch6"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch6"],11))+" A" ])
            tableData.append([ "Ch7", str(round(hv_Data_Avg["VoltageB0"]["Ch7"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch7"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch7"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch7"],11))+" A" ])
            tableData.append([ "Ch8", str(round(hv_Data_Avg["VoltageB0"]["Ch8"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch8"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch8"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch8"],11))+" A" ])
            tableData.append([ "Ch9", str(round(hv_Data_Avg["VoltageB0"]["Ch9"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch9"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch9"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch9"],11))+" A" ])
            tableData.append([ "Ch10", str(round(hv_Data_Avg["VoltageB0"]["Ch10"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch10"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch10"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch10"],11))+" A" ])
            tableData.append([ "Ch11", str(round(hv_Data_Avg["VoltageB0"]["Ch11"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch11"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch11"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch11"],11))+" A" ])
            tableData.append([ "Ch12", str(round(hv_Data_Avg["VoltageB0"]["Ch12"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch12"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch12"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch12"],11))+" A" ])
            tableData.append([ "Ch13", str(round(hv_Data_Avg["VoltageB0"]["Ch13"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch13"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch13"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch13"],11))+" A" ])
            tableData.append([ "Ch14", str(round(hv_Data_Avg["VoltageB0"]["Ch14"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch14"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch14"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch14"],11))+" A" ])
            tableData.append([ "Ch15", str(round(hv_Data_Avg["VoltageB0"]["Ch15"],2)) + " V", str(round(hv_Data_Avg["CurrentB0"]["Ch15"],11))+" A" , str(round(hv_Data_Avg["VoltageB1"]["Ch15"],2)) + " V", str(round(hv_Data_Avg["CurrentB1"]["Ch15"],11))+" A" ])

            Table = Texttable()
            Table.add_rows(tableData)
            TableText = Table.draw()
            print(TableText)

            CurrentTime = datetime.now()
            print("Time-> {} => Module-> {} => Data point-> {} => DB-> {} => Log-> {}".format(CurrentTime, ModuleName, ReadNumber, databaseStatus, logstatus), end='\r')

            time.sleep(0.5)
            os.system('clear')
            tableData = []
        except KeyError:
            pass

def dataAcquisitionAvg(hv_DataContainer):
    for key in hv_DataContainer[0]:
        for ch in hv_DataContainer[0][key]:
            for index in range(1, len(hv_DataContainer)):
                hv_DataContainer[0][key][ch] += hv_DataContainer[index][key][ch]
            hv_DataContainer[0][key][ch] = hv_DataContainer[0][key][ch]/len(hv_DataContainer)
    return hv_DataContainer[0]

def dataAcquisition(HV_boards, lognameB0, lognameB1):
    global deltaTime, timeStampContainer, influxdbContainer, hv_DataContainer, ReadNumber, hv_Data_Avg

    while True:
        ############### Time in UTC ################
        CurrentTime = datetime.fromtimestamp(datetime.utcnow().timestamp())
        timeStampContainer.append(CurrentTime)

        startTime = CurrentTime
        instant_hv_Data = HV_boards.readDeviceData()
        hv_DataContainer.append(instant_hv_Data)
        time.sleep(Delay)
        stopTime = datetime.fromtimestamp(datetime.utcnow().timestamp())

        deltaTime += timedelta.total_seconds(stopTime-startTime)

        if logstatus == "on":
            f1 = open(lognameB0, "a")
            f2 = open(lognameB1, "a")

            f1.write("{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{}\n".format(
                CurrentTime,
                instant_hv_Data['VoltageB0']['Ch0'],
                instant_hv_Data['CurrentB0']['Ch0'],
                instant_hv_Data['VoltageB0']['Ch1'],
                instant_hv_Data['CurrentB0']['Ch1'],
                instant_hv_Data['VoltageB0']['Ch2'],
                instant_hv_Data['CurrentB0']['Ch2'],
                instant_hv_Data['VoltageB0']['Ch3'],
                instant_hv_Data['CurrentB0']['Ch3'],
                instant_hv_Data['VoltageB0']['Ch4'],
                instant_hv_Data['CurrentB0']['Ch4'],
                instant_hv_Data['VoltageB0']['Ch5'],
                instant_hv_Data['CurrentB0']['Ch5'],
                instant_hv_Data['VoltageB0']['Ch6'],
                instant_hv_Data['CurrentB0']['Ch6'],
                instant_hv_Data['VoltageB0']['Ch7'],
                instant_hv_Data['CurrentB0']['Ch7'],
                instant_hv_Data['VoltageB0']['Ch8'],
                instant_hv_Data['CurrentB0']['Ch8'],
                instant_hv_Data['VoltageB0']['Ch9'],
                instant_hv_Data['CurrentB0']['Ch9'],
                instant_hv_Data['VoltageB0']['Ch10'],
                instant_hv_Data['CurrentB0']['Ch10'],
                instant_hv_Data['VoltageB0']['Ch11'],
                instant_hv_Data['CurrentB0']['Ch11'],
                instant_hv_Data['VoltageB0']['Ch12'],
                instant_hv_Data['CurrentB0']['Ch12'],
                instant_hv_Data['VoltageB0']['Ch13'],
                instant_hv_Data['CurrentB0']['Ch13'],
                instant_hv_Data['VoltageB0']['Ch14'],
                instant_hv_Data['CurrentB0']['Ch14'],
                instant_hv_Data['VoltageB0']['Ch15'],
                instant_hv_Data['CurrentB0']['Ch15']))
            
            f2.write("{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{}\n".format(
                CurrentTime,
                instant_hv_Data['VoltageB1']['Ch0'],
                instant_hv_Data['CurrentB1']['Ch0'],
                instant_hv_Data['VoltageB1']['Ch1'],
                instant_hv_Data['CurrentB1']['Ch1'],
                instant_hv_Data['VoltageB1']['Ch2'],
                instant_hv_Data['CurrentB1']['Ch2'],
                instant_hv_Data['VoltageB1']['Ch3'],
                instant_hv_Data['CurrentB1']['Ch3'],
                instant_hv_Data['VoltageB1']['Ch4'],
                instant_hv_Data['CurrentB1']['Ch4'],
                instant_hv_Data['VoltageB1']['Ch5'],
                instant_hv_Data['CurrentB1']['Ch5'],
                instant_hv_Data['VoltageB1']['Ch6'],
                instant_hv_Data['CurrentB1']['Ch6'],
                instant_hv_Data['VoltageB1']['Ch7'],
                instant_hv_Data['CurrentB1']['Ch7'],
                instant_hv_Data['VoltageB1']['Ch8'],
                instant_hv_Data['CurrentB1']['Ch8'],
                instant_hv_Data['VoltageB1']['Ch9'],
                instant_hv_Data['CurrentB1']['Ch9'],
                instant_hv_Data['VoltageB1']['Ch10'],
                instant_hv_Data['CurrentB1']['Ch10'],
                instant_hv_Data['VoltageB1']['Ch11'],
                instant_hv_Data['CurrentB1']['Ch11'],
                instant_hv_Data['VoltageB1']['Ch12'],
                instant_hv_Data['CurrentB1']['Ch12'],
                instant_hv_Data['VoltageB1']['Ch13'],
                instant_hv_Data['CurrentB1']['Ch13'],
                instant_hv_Data['VoltageB1']['Ch14'],
                instant_hv_Data['CurrentB1']['Ch14'],
                instant_hv_Data['VoltageB1']['Ch15'],
                instant_hv_Data['CurrentB1']['Ch15']))

            f1.close()
            f2.close()

        if deltaTime >= 1:
            deltaTime = 0
            avgMiddleTime = timeStampContainer[0] + (timeStampContainer[-1] - timeStampContainer[0])/2
            hv_Data_Avg = dataAcquisitionAvg(hv_DataContainer)

            if TermPrint == "off":
                print("Time-> {} => Module-> {} => Data point-> {} => DB-> {} => Log-> {}".format(CurrentTime, ModuleName, ReadNumber, databaseStatus, logstatus), end='\r')

            if databaseStatus == "on":
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch7'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch7']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch6'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch6']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch15'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch15']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch14'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch14']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB0"]['Ch7'],
                            "Current" : hv_Data_Avg["CurrentB0"]['Ch7']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB0"]['Ch6'],
                            "Current" : hv_Data_Avg["CurrentB0"]['Ch6']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch5'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch5']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch4'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch4']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch13'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch13']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch12'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch12']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB0"]['Ch5'],
                            "Current" : hv_Data_Avg["CurrentB0"]['Ch5']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB0"]['Ch4'],
                            "Current" : hv_Data_Avg["CurrentB0"]['Ch4']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch3'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch3']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch2'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch2']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch11'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch11']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch10'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch10']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB0"]['Ch3'],
                            "Current" : hv_Data_Avg["CurrentB0"]['Ch3']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB0"]['Ch2'],
                            "Current" : hv_Data_Avg["CurrentB0"]['Ch2']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch1'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch1']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch0'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch0']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch9'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch9']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB1"]['Ch8'],
                            "Current" : hv_Data_Avg["CurrentB1"]['Ch8']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB0"]['Ch1'],
                            "Current" : hv_Data_Avg["CurrentB0"]['Ch1']
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
                        "time": avgMiddleTime,
                        "fields": {
                            "Voltage" : hv_Data_Avg["VoltageB0"]['Ch0'],
                            "Current" : hv_Data_Avg["CurrentB0"]['Ch0']
                        }
                    }
                )

                if {'name' : databaseName} in dbclient.get_list_database():
                    dbclient.write_points(influxdbContainer, database=databaseName)
                    pass

                else:
                    dbclient.create_database(databaseName)
                    print("Creating database ...")
                    dbclient.write_points(influxdbContainer, database=databaseName)
                    print("Finished writing to InfluxDB")

                influxdbContainer = []

            hv_DataContainer = []
            timeStampContainer = []
            ReadNumber += 1

if __name__ == '__main__':
    HV_boards = HVBoard()

    if logstatus == "on":
        startTimeStamp = datetime.fromtimestamp(datetime.utcnow().timestamp()).strftime('%Y-%m-%d_%H:%M:%S')
        lognameB0 = "{}_HV_B0_{}.csv".format(ModuleName, str(startTimeStamp))
        lognameB1 = "{}_HV_B1_{}.csv".format(ModuleName, str(startTimeStamp))
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

    dataAcquisition_thread = threading.Thread(target=dataAcquisition, args=(HV_boards,lognameB0,lognameB1,))
    if TermPrint == "on":
        printData_thread = threading.Thread(target=printDataTable)
    dataAcquisition_thread.start()
    if TermPrint == "on":
        printData_thread.start()