#!/usr/bin/env python3
from influxdb import InfluxDBClient
from datetime import timezone
from datetime import datetime
import argparse
import arrow
import pytz

############################ Requirement ##############################
# Source Code: https://github.com/zazachubin/MM-Grafana-integration
# python3 -m pip install arrow
# python3 -m pip install influxdb-client
# python3 -m pip install influxdb
################################ Run ##################################
# -- run new strategy board connectivity
# python3 HV_log_Uploader2Database.py --file <path>/<filename> --module <ModuleName>

def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        description='HV server logger')
    parser.add_argument('--file', type=str, required=False,
                        default='',
                        help='File name')
    parser.add_argument('--module', type=str, required=False,
                        default='M_test',
                        help='Module name')
    parser.add_argument('--w', type=int, required=False,
                        default=1000,
                        help='Data transfer window size to database')
    return parser.parse_args()

args = parse_args()
ModuleName = args.module
FileName = args.file
windowSize = args.w

print("Module Name: {}".format(ModuleName))
print("File name: {}".format(FileName))

############################## InfluxDB ###############################
InfluxDB_ADDRESS = 'bes3.jinr.ru'  # InfluxDB Local address
databaseName = 'MM_Dubna'          # Database name
username = 'MM_Dubna'              # Database user name          
password = '********'              # Database password
DB_port = 8086                     # Database port

dbclient = InfluxDBClient(  host=InfluxDB_ADDRESS,
                            port=DB_port,
                            username=username,
                            password=password,
                            database=databaseName)

################# Russian time stamp to UTC converter #################
def Ru_to_UTC(Ru_timestamp):
    timestamp_Ru = datetime.strptime(Ru_timestamp, '%d.%m.%Y %H:%M:%S')
    timezone = pytz.timezone('Europe/Moscow')
    dt = arrow.get(timezone.localize(timestamp_Ru)).to('UTC')
    convertedStamp = str(dt.date()) + ' ' + str(dt.time())
    Ru_to_UTC = datetime.strptime(convertedStamp, '%Y-%m-%d %H:%M:%S')
    return Ru_to_UTC
############################# ProgressBar #############################
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print("\r{} |{}| {}% {}".format(prefix,bar,percent,suffix), end='\r')
    # Print New Line on Complete
    if iteration == total: 
        print()
############################## Data Load ##############################

############### Time ##############
Time = []
######### Board 1 ########
# current Ch1-Ch6
S8_2R_1_I = []
S8_2L_2_I = []
S7_2R_3_I = []
S7_2L_4_I = []
S6_2R_5_I = []
S6_2L_6_I = []
# Voltage Ch1-Ch6
S8_2R_1_V = []
S8_2L_2_V = []
S7_2R_3_V = []
S7_2L_4_V = []
S6_2R_5_V = []
S6_2L_6_V = []
# current Ch7-Ch12
S8_1R_7_I = []
S8_1L_8_I = []
S7_1R_9_I = []
S7_1L_10_I = []
S6_1R_11_I = []
S6_1L_12_I = []
# Voltage Ch7-Ch12
S8_1R_7_V = []
S8_1L_8_V = []
S7_1R_9_V = []
S7_1L_10_V = []
S6_1R_11_V = []
S6_1L_12_V = []
##########################
######### Board 2 ########
# current Ch13-Ch18
E8_2R_13_I = []
E8_2L_14_I = []
E7_2R_15_I = []
E7_2L_16_I = []
E6_2R_17_I = []
E6_2L_18_I = []
# Voltage Ch13-Ch18
E8_2R_13_V = []
E8_2L_14_V = []
E7_2R_15_V = []
E7_2L_16_V = []
E6_2R_17_V = []
E6_2L_18_V = []
# current Ch19-Ch24
E8_1R_19_I = []
E8_1L_20_I = []
E7_1R_21_I = []
E7_1L_22_I = []
E6_1R_23_I = []
E6_1L_24_I = []
# Voltage Ch19-Ch24
E8_1R_19_V = []
E8_1L_20_V = []
E7_1R_21_V = []
E7_1L_22_V = []
E6_1R_23_V = []
E6_1L_24_V = []
##########################
Temperature = []
Pressure = []

def stringToFloat(string):
    splitedString = string.split(",")
    buildString = splitedString[0] + '.' + splitedString[1]
    return(float(buildString))

def loadData(FileName):
    counter = 0
    file = open(FileName,"r")
    for line in file:
        fields = line.split("\t")
            ############### Time ##############
        try:
            Time.append(Ru_to_UTC(fields[0]))
        except ValueError:
            pass
            ############## Board 1 ############
        try:
            # current Ch1-Ch6
            S8_2R_1_I.append(stringToFloat(fields[1]))
            S8_2L_2_I.append(stringToFloat(fields[2]))
            S7_2R_3_I.append(stringToFloat(fields[3]))
            S7_2L_4_I.append(stringToFloat(fields[4]))
            S6_2R_5_I.append(stringToFloat(fields[5]))
            S6_2L_6_I.append(stringToFloat(fields[6]))
            # Voltage Ch1-Ch6
            S8_2R_1_V.append(stringToFloat(fields[7]))
            S8_2L_2_V.append(stringToFloat(fields[8]))
            S7_2R_3_V.append(stringToFloat(fields[9]))
            S7_2L_4_V.append(stringToFloat(fields[10]))
            S6_2R_5_V.append(stringToFloat(fields[11]))
            S6_2L_6_V.append(stringToFloat(fields[12]))
            # current Ch7-Ch12
            S8_1R_7_I.append(stringToFloat(fields[13]))
            S8_1L_8_I.append(stringToFloat(fields[14]))
            S7_1R_9_I.append(stringToFloat(fields[15]))
            S7_1L_10_I.append(stringToFloat(fields[16]))
            S6_1R_11_I.append(stringToFloat(fields[17]))
            S6_1L_12_I.append(stringToFloat(fields[18]))
            # Voltage Ch7-Ch12
            S8_1R_7_V.append(stringToFloat(fields[19]))
            S8_1L_8_V.append(stringToFloat(fields[20]))
            S7_1R_9_V.append(stringToFloat(fields[21]))
            S7_1L_10_V.append(stringToFloat(fields[22]))
            S6_1R_11_V.append(stringToFloat(fields[23]))
            S6_1L_12_V.append(stringToFloat(fields[24]))
            ###################################
            ############## Board 2 ############
            # current Ch13-Ch18
            E8_2R_13_I.append(stringToFloat(fields[25]))
            E8_2L_14_I.append(stringToFloat(fields[26]))
            E7_2R_15_I.append(stringToFloat(fields[27]))
            E7_2L_16_I.append(stringToFloat(fields[28]))
            E6_2R_17_I.append(stringToFloat(fields[29]))
            E6_2L_18_I.append(stringToFloat(fields[30]))
            # Voltage Ch13-Ch18
            E8_2R_13_V.append(stringToFloat(fields[31]))
            E8_2L_14_V.append(stringToFloat(fields[32]))
            E7_2R_15_V.append(stringToFloat(fields[33]))
            E7_2L_16_V.append(stringToFloat(fields[34]))
            E6_2R_17_V.append(stringToFloat(fields[35]))
            E6_2L_18_V.append(stringToFloat(fields[36]))
            # current Ch19-Ch24
            E8_1R_19_I.append(stringToFloat(fields[37]))
            E8_1L_20_I.append(stringToFloat(fields[38]))
            E7_1R_21_I.append(stringToFloat(fields[39]))
            E7_1L_22_I.append(stringToFloat(fields[40]))
            E6_1R_23_I.append(stringToFloat(fields[41]))
            E6_1L_24_I.append(stringToFloat(fields[42]))
            # Voltage Ch19-Ch24
            E8_1R_19_V.append(stringToFloat(fields[43]))
            E8_1L_20_V.append(stringToFloat(fields[44]))
            E7_1R_21_V.append(stringToFloat(fields[45]))
            E7_1L_22_V.append(stringToFloat(fields[46]))
            E6_1R_23_V.append(stringToFloat(fields[47]))
            E6_1L_24_V.append(stringToFloat(fields[48]))
            ####################################
            ############ Temperature ###########
            Temperature.append(stringToFloat(fields[49]))
            ############# Pressure #############
            Pressure.append(stringToFloat(fields[50]))
            counter+=1
            print("Points: {}".format(counter), end='\r')
        except IndexError:
            pass
    file.close()

# Load data from file
loadData(FileName)

dataLength = len(Time)
start = 0
stop = windowSize
windowNumber = int(dataLength/windowSize)+1
print("\nData chunks number: {}".format(windowNumber))
##################### Data formater for dtatabase #####################
for i in range(1, windowNumber):
    influxdbContainer = []
    for j in range(len(Time[start:stop])):
        ################### L1 #####################
        ## PCB L8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "L8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S8_2L_2_V[start:stop][j],
                    "Current" : S8_2L_2_I[start:stop][j]
                }
            }
        )
        ## PCB R8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "R8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S8_2R_1_V[start:stop][j],
                    "Current" : S8_2R_1_I[start:stop][j]
                }
            }
        )
        ## PCB L7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "L7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S7_2L_4_V[start:stop][j],
                    "Current" : S7_2L_4_I[start:stop][j]
                }
            }
        )
        ## PCB R7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "R7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S7_2R_3_V[start:stop][j],
                    "Current" : S7_2R_3_I[start:stop][j]
                }
            }
        )
        ## PCB L6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "L6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S6_2L_6_V[start:stop][j],
                    "Current" : S6_2L_6_I[start:stop][j]
                }
            }
        )
        ## PCB R6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "R6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S6_2R_5_V[start:stop][j],
                    "Current" : S6_2R_5_I[start:stop][j]
                }
            }
        )
        ################### L2 #####################
        ## PCB L8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "L8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S8_1L_8_V[start:stop][j],
                    "Current" : S8_1L_8_I[start:stop][j]
                }
            }
        )
        ## PCB R8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "R8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S8_1R_7_V[start:stop][j],
                    "Current" : S8_1R_7_I[start:stop][j]
                }
            }
        )
        ## PCB L7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "L7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S7_1L_10_V[start:stop][j],
                    "Current" : S7_1L_10_I[start:stop][j]
                }
            }
        )
        ## PCB R7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "R7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S7_1R_9_V[start:stop][j],
                    "Current" : S7_1R_9_I[start:stop][j]
                }
            }
        )
        ## PCB L6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "L6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S6_1L_12_V[start:stop][j],
                    "Current" : S6_1L_12_I[start:stop][j]
                }
            }
        )
        ## PCB R6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "R6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S6_1R_11_V[start:stop][j],
                    "Current" : S6_1R_11_I[start:stop][j]
                }
            }
        )
        ################### L3 #####################
        ## PCB L8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "L8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E8_2L_14_V[start:stop][j],
                    "Current" : E8_2L_14_I[start:stop][j]
                }
            }
        )
        ## PCB R8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "R8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E8_2R_13_V[start:stop][j],
                    "Current" : E8_2R_13_I[start:stop][j]
                }
            }
        )
        ## PCB L7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "L7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E7_2L_16_V[start:stop][j],
                    "Current" : E7_2L_16_I[start:stop][j]
                }
            }
        )
        ## PCB R7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "R7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E7_2R_15_V[start:stop][j],
                    "Current" : E7_2R_15_I[start:stop][j]
                }
            }
        )
        ## PCB L6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "L6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E6_2L_18_V[start:stop][j],
                    "Current" : E6_2L_18_I[start:stop][j]
                }
            }
        )
        ## PCB R6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "R6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E6_2R_17_V[start:stop][j],
                    "Current" : E6_2R_17_I[start:stop][j]
                }
            }
        )
        ################### L4 #####################
        ## PCB L8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "L8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E8_1L_20_V[start:stop][j],
                    "Current" : E8_1L_20_I[start:stop][j]
                }
            }
        )
        ## PCB R8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "R8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E8_1R_19_V[start:stop][j],
                    "Current" : E8_1R_19_I[start:stop][j]
                }
            }
        )
        ## PCB L7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "L7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E7_1L_22_V[start:stop][j],
                    "Current" : E7_1L_22_I[start:stop][j]
                }
            }
        )
        ## PCB R7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "R7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E7_1R_21_V[start:stop][j],
                    "Current" : E7_1R_21_I[start:stop][j]
                }
            }
        )
        ## PCB L6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "L6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E6_1L_24_V[start:stop][j],
                    "Current" : E6_1L_24_I[start:stop][j]
                }
            }
        )
        ## PCB R6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "R6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E6_1R_23_V[start:stop][j],
                    "Current" : E6_1R_23_I[start:stop][j]
                }
            }
        )
        ###################################################
        ## Temperature
        influxdbContainer.append(
            {
                "measurement": "CLEAN_ROOM_ENVIRONMENT",
                "tags": {
                    "Module" : ModuleName,
                    "Parameter" : "Temperature"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Temperature" : Temperature[start:stop][j]
                }
            }
        )
        ## Pressure
        influxdbContainer.append(
            {
                "measurement": "CLEAN_ROOM_ENVIRONMENT",
                "tags": {
                    "Module" : ModuleName,
                    "Parameter" : "Pressure"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Pressure" : Pressure[start:stop][j]
                }
            }
        )

        printProgressBar(j, len(Time[start:stop]), prefix = "Progress: {}/{} -> Module:{} -> UTC-Time: {}".format(i, windowNumber, ModuleName, Time[start:stop][-1]), suffix = 'Complete', length = 25)

    start += windowSize
    stop += windowSize

    if {'name' : databaseName} in dbclient.get_list_database():
        dbclient.write_points(influxdbContainer, database=databaseName)
    else:
        dbclient.create_database(databaseName)
        print("Creating database ...")
        dbclient.write_points(influxdbContainer, database=databaseName)
        print("Finished writing to InfluxDB")
print()
if dataLength-(stop-windowSize) != 0:
    stop = start+(dataLength-(stop-windowSize))
    influxdbContainer = []
    for j in range(len(Time[start:stop])):
        ################### L1 #####################
        ## PCB L8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "L8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S8_2L_2_V[start:stop][j],
                    "Current" : S8_2L_2_I[start:stop][j]
                }
            }
        )
        ## PCB R8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "R8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S8_2R_1_V[start:stop][j],
                    "Current" : S8_2R_1_I[start:stop][j]
                }
            }
        )
        ## PCB L7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "L7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S7_2L_4_V[start:stop][j],
                    "Current" : S7_2L_4_I[start:stop][j]
                }
            }
        )
        ## PCB R7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "R7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S7_2R_3_V[start:stop][j],
                    "Current" : S7_2R_3_I[start:stop][j]
                }
            }
        )
        ## PCB L6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "L6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S6_2L_6_V[start:stop][j],
                    "Current" : S6_2L_6_I[start:stop][j]
                }
            }
        )
        ## PCB R6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L1",
                    "PCB" : "R6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S6_2R_5_V[start:stop][j],
                    "Current" : S6_2R_5_I[start:stop][j]
                }
            }
        )
        ################### L2 #####################
        ## PCB L8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "L8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S8_1L_8_V[start:stop][j],
                    "Current" : S8_1L_8_I[start:stop][j]
                }
            }
        )
        ## PCB R8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "R8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S8_1R_7_V[start:stop][j],
                    "Current" : S8_1R_7_I[start:stop][j]
                }
            }
        )
        ## PCB L7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "L7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S7_1L_10_V[start:stop][j],
                    "Current" : S7_1L_10_I[start:stop][j]
                }
            }
        )
        ## PCB R7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "R7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S7_1R_9_V[start:stop][j],
                    "Current" : S7_1R_9_I[start:stop][j]
                }
            }
        )
        ## PCB L6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "L6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S6_1L_12_V[start:stop][j],
                    "Current" : S6_1L_12_I[start:stop][j]
                }
            }
        )
        ## PCB R6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L2",
                    "PCB" : "R6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : S6_1R_11_V[start:stop][j],
                    "Current" : S6_1R_11_I[start:stop][j]
                }
            }
        )
        ################### L3 #####################
        ## PCB L8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "L8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E8_2L_14_V[start:stop][j],
                    "Current" : E8_2L_14_I[start:stop][j]
                }
            }
        )
        ## PCB R8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "R8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E8_2R_13_V[start:stop][j],
                    "Current" : E8_2R_13_I[start:stop][j]
                }
            }
        )
        ## PCB L7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "L7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E7_2L_16_V[start:stop][j],
                    "Current" : E7_2L_16_I[start:stop][j]
                }
            }
        )
        ## PCB R7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "R7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E7_2R_15_V[start:stop][j],
                    "Current" : E7_2R_15_I[start:stop][j]
                }
            }
        )
        ## PCB L6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "L6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E6_2L_18_V[start:stop][j],
                    "Current" : E6_2L_18_I[start:stop][j]
                }
            }
        )
        ## PCB R6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L3",
                    "PCB" : "R6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E6_2R_17_V[start:stop][j],
                    "Current" : E6_2R_17_I[start:stop][j]
                }
            }
        )
        ################### L4 #####################
        ## PCB L8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "L8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E8_1L_20_V[start:stop][j],
                    "Current" : E8_1L_20_I[start:stop][j]
                }
            }
        )
        ## PCB R8
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "R8"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E8_1R_19_V[start:stop][j],
                    "Current" : E8_1R_19_I[start:stop][j]
                }
            }
        )
        ## PCB L7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "L7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E7_1L_22_V[start:stop][j],
                    "Current" : E7_1L_22_I[start:stop][j]
                }
            }
        )
        ## PCB R7
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "R7"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E7_1R_21_V[start:stop][j],
                    "Current" : E7_1R_21_I[start:stop][j]
                }
            }
        )
        ## PCB L6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "L6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E6_1L_24_V[start:stop][j],
                    "Current" : E6_1L_24_I[start:stop][j]
                }
            }
        )
        ## PCB R6
        influxdbContainer.append(
            {
                "measurement": "HV_CLEAN_ROOM",
                "tags": {
                    "Module" : ModuleName,
                    "Layer" : "L4",
                    "PCB" : "R6"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Voltage" : E6_1R_23_V[start:stop][j],
                    "Current" : E6_1R_23_I[start:stop][j]
                }
            }
        )
        ###################################################
        ## Temperature
        influxdbContainer.append(
            {
                "measurement": "CLEAN_ROOM_ENVIRONMENT",
                "tags": {
                    "Module" : ModuleName,
                    "Parameter" : "Temperature"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Temperature" : Temperature[start:stop][j]
                }
            }
        )
        ## Pressure
        influxdbContainer.append(
            {
                "measurement": "CLEAN_ROOM_ENVIRONMENT",
                "tags": {
                    "Module" : ModuleName,
                    "Parameter" : "Pressure"
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Pressure" : Pressure[start:stop][j]
                }
            }
        )
    if {'name' : databaseName} in dbclient.get_list_database():
        dbclient.write_points(influxdbContainer, database=databaseName)
    else:
        dbclient.create_database(databaseName)
        print("Creating database ...")
        dbclient.write_points(influxdbContainer, database=databaseName)
        print("Finished writing to InfluxDB")