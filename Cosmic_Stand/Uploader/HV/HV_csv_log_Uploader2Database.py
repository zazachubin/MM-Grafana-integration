#!/usr/bin/env python3
from influxdb import InfluxDBClient
from datetime import timezone
from datetime import datetime
import argparse
import arrow
import pytz
import csv

############################ Requirement ##############################
# Source Code: https://github.com/zazachubin/MM-Grafana-integration
# python3 -m pip install arrow
# python3 -m pip install influxdb
# python3 -m pip install influxdb-client
################################ Run ##################################
# -- run new strategy board connectivity
# python3 HV_csv_log_Uploader2Database.py --file <path>/<filename> --module <ModuleName> --board <B1 or B0>

# -- run old strategy board connectivity
# python3 HV_csv_log_Uploader2Database.py --file <path>/<filename> --module <ModuleName> --board <B1 or B0> --connect <old or new>

def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        description='HV logs uploader')
    parser.add_argument('--file', type=str, required=False,
                        default='',
                        help='File name')
    parser.add_argument('--module', type=str, required=False,
                        default='M_test',
                        help='Module name')
    parser.add_argument('--board', type=str, required=False,
                        default='B1',
                        help='HV Board name')
    parser.add_argument('--w', type=int, required=False,
                        default=10000,
                        help='Data transfer window size to database')
    parser.add_argument('--connect', type=str, required=False,
                        default='new',
                        help='HV boards connection strategy (new, old)')
    return parser.parse_args()

args = parse_args()
ModuleName = args.module
Board = args.board
FileName = args.file
windowSize = args.w
connectStrategy = args.connect

print("Module Name: {}".format(ModuleName))
print("File name: {}".format(FileName))
print("Board: {}".format(Board))
print("Connect strategy: {}".format(connectStrategy))

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
    timestamp_Ru = datetime.strptime(Ru_timestamp, '%Y-%m-%d %H:%M:%S.%f')
    timezone = pytz.timezone('Europe/Moscow')
    dt = arrow.get(timezone.localize(timestamp_Ru)).to('UTC')
    convertedStamp = str(dt.date()) + ' ' + str(dt.time())
    try:
        Ru_to_UTC = datetime.strptime(convertedStamp, '%Y-%m-%d %H:%M:%S.%f')
    except:
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

Time = []
v_ch0 = []
i_ch0 = []
v_ch1 = []
i_ch1 = []
v_ch2 = []
i_ch2 = []
v_ch3 = []
i_ch3 = []
v_ch4 = []
i_ch4 = []
v_ch5 = []
i_ch5 = []
v_ch6 = []
i_ch6 = []
v_ch7 = []
i_ch7 = []
v_ch8 = []
i_ch8 = []
v_ch9 = []
i_ch9 = []
v_ch10 = []
i_ch10 = []
v_ch11 = []
i_ch11 = []
v_ch12 = []
i_ch12 = []
v_ch13 = []
i_ch13 = []
v_ch14 = []
i_ch14 = []
v_ch15 = []
i_ch15 = []

counter = 0
with open(FileName) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    for row in csv_reader:
        if counter != 0:
            Time.append(Ru_to_UTC(row[0]+' '+row[1]))
            v_ch0.append(float(row[3].replace(',','.')))
            i_ch0.append(float(row[4].replace(',','.')))
            v_ch1.append(float(row[5].replace(',','.')))
            i_ch1.append(float(row[6].replace(',','.')))
            v_ch2.append(float(row[7].replace(',','.')))
            i_ch2.append(float(row[8].replace(',','.')))
            v_ch3.append(float(row[9].replace(',','.')))
            i_ch3.append(float(row[10].replace(',','.')))
            v_ch4.append(float(row[11].replace(',','.')))
            i_ch4.append(float(row[12].replace(',','.')))
            v_ch5.append(float(row[13].replace(',','.')))
            i_ch5.append(float(row[14].replace(',','.')))
            v_ch6.append(float(row[15].replace(',','.')))
            i_ch6.append(float(row[16].replace(',','.')))
            v_ch7.append(float(row[17].replace(',','.')))
            i_ch7.append(float(row[18].replace(',','.')))
            v_ch8.append(float(row[19].replace(',','.')))
            i_ch8.append(float(row[20].replace(',','.')))
            v_ch9.append(float(row[21].replace(',','.')))
            i_ch9.append(float(row[22].replace(',','.')))
            v_ch10.append(float(row[23].replace(',','.')))
            i_ch10.append(float(row[24].replace(',','.')))
            v_ch11.append(float(row[25].replace(',','.')))
            i_ch11.append(float(row[26].replace(',','.')))
            v_ch12.append(float(row[27].replace(',','.')))
            i_ch12.append(float(row[28].replace(',','.')))
            v_ch13.append(float(row[29].replace(',','.')))
            i_ch13.append(float(row[30].replace(',','.')))
            v_ch14.append(float(row[31].replace(',','.')))
            i_ch14.append(float(row[32].replace(',','.')))
            v_ch15.append(float(row[33].replace(',','.')))
            i_ch15.append(float(row[34].replace(',','.')))
        counter+=1
        print("Points: {}".format(counter), end='\r')

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
        if Board == "B1":
            ## PCB L8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L1",
                        "PCB" : "L8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch7[start:stop][j],
                        "Current" : i_ch7[start:stop][j]
                    }
                }
            )
            ## PCB R8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L1",
                        "PCB" : "R8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch6[start:stop][j],
                        "Current" : i_ch6[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'new':
                ## PCB L7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L1",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch15[start:stop][j],
                            "Current" : i_ch15[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L1",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch14[start:stop][j],
                            "Current" : i_ch14[start:stop][j]
                        }
                    }
                )
        elif Board == "B0":
            ## PCB L6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L1",
                        "PCB" : "L6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch7[start:stop][j],
                        "Current" : i_ch7[start:stop][j]
                    }
                }
            )
            ## PCB R6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L1",
                        "PCB" : "R6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch6[start:stop][j],
                        "Current" : i_ch6[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'old':
                ## PCB L7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L1",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch15[start:stop][j],
                            "Current" : i_ch15[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L1",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch14[start:stop][j],
                            "Current" : i_ch14[start:stop][j]
                        }
                    }
                )
        ################### L2 #####################
        if Board == "B1":
            ## PCB L8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L2",
                        "PCB" : "L8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch5[start:stop][j],
                        "Current" : i_ch5[start:stop][j]
                    }
                }
            )
            ## PCB R8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L2",
                        "PCB" : "R8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch4[start:stop][j],
                        "Current" : i_ch4[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'new':
                ## PCB L7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L2",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch13[start:stop][j],
                            "Current" : i_ch13[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L2",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch12[start:stop][j],
                            "Current" : i_ch12[start:stop][j]
                        }
                    }
                )
        elif Board == "B0":
            ## PCB L6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L2",
                        "PCB" : "L6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch5[start:stop][j],
                        "Current" : i_ch5[start:stop][j]
                    }
                }
            )
            ## PCB R6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L2",
                        "PCB" : "R6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch4[start:stop][j],
                        "Current" : i_ch4[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'old':
                ## PCB L7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L2",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch13[start:stop][j],
                            "Current" : i_ch13[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L2",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch12[start:stop][j],
                            "Current" : i_ch12[start:stop][j]
                        }
                    }
                )
        ################### L3 #####################
        if Board == "B1":
            ## PCB L8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L3",
                        "PCB" : "L8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch3[start:stop][j],
                        "Current" : i_ch3[start:stop][j]
                    }
                }
            )
            ## PCB R8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L3",
                        "PCB" : "R8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch2[start:stop][j],
                        "Current" : i_ch2[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'new':
                ## PCB L7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L3",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch11[start:stop][j],
                            "Current" : i_ch11[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L3",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch10[start:stop][j],
                            "Current" : i_ch10[start:stop][j]
                        }
                    }
                )
        elif Board == "B0":
            ## PCB L6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L3",
                        "PCB" : "L6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch3[start:stop][j],
                        "Current" : i_ch3[start:stop][j]
                    }
                }
            )
            ## PCB R6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L3",
                        "PCB" : "R6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch2[start:stop][j],
                        "Current" : i_ch2[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'old':
                ## PCB L7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L3",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch11[start:stop][j],
                            "Current" : i_ch11[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L3",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch10[start:stop][j],
                            "Current" : i_ch10[start:stop][j]
                        }
                    }
                )
        ################### L4 #####################
        if Board == "B1":
            ## PCB L8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L4",
                        "PCB" : "L8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch1[start:stop][j],
                        "Current" : i_ch1[start:stop][j]
                    }
                }
            )
            ## PCB R8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L4",
                        "PCB" : "R8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch0[start:stop][j],
                        "Current" : i_ch0[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'new':
                ## PCB L7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L4",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch9[start:stop][j],
                            "Current" : i_ch9[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L4",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch8[start:stop][j],
                            "Current" : i_ch8[start:stop][j]
                        }
                    }
                )
        elif Board == "B0":
            ## PCB L6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L4",
                        "PCB" : "L6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch1[start:stop][j],
                        "Current" : i_ch1[start:stop][j]
                    }
                }
            )
            ## PCB R6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L4",
                        "PCB" : "R6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch0[start:stop][j],
                        "Current" : i_ch0[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'old':
                ## PCB L7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L4",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch9[start:stop][j],
                            "Current" : i_ch9[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L4",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch8[start:stop][j],
                            "Current" : i_ch8[start:stop][j]
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
        if Board == "B1":
            ## PCB L8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L1",
                        "PCB" : "L8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch7[start:stop][j],
                        "Current" : i_ch7[start:stop][j]
                    }
                }
            )
            ## PCB R8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L1",
                        "PCB" : "R8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch6[start:stop][j],
                        "Current" : i_ch6[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'new':
                ## PCB L7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L1",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch15[start:stop][j],
                            "Current" : i_ch15[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L1",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch14[start:stop][j],
                            "Current" : i_ch14[start:stop][j]
                        }
                    }
                )
        elif Board == "B0":
            ## PCB L6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L1",
                        "PCB" : "L6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch7[start:stop][j],
                        "Current" : i_ch7[start:stop][j]
                    }
                }
            )
            ## PCB R6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L1",
                        "PCB" : "R6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch6[start:stop][j],
                        "Current" : i_ch6[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'old':
                ## PCB L7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L1",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch15[start:stop][j],
                            "Current" : i_ch15[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L1",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch14[start:stop][j],
                            "Current" : i_ch14[start:stop][j]
                        }
                    }
                )
        ################### L2 #####################
        if Board == "B1":
            ## PCB L8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L2",
                        "PCB" : "L8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch5[start:stop][j],
                        "Current" : i_ch5[start:stop][j]
                    }
                }
            )
            ## PCB R8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L2",
                        "PCB" : "R8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch4[start:stop][j],
                        "Current" : i_ch4[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'new':
                ## PCB L7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L2",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch13[start:stop][j],
                            "Current" : i_ch13[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L2",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch12[start:stop][j],
                            "Current" : i_ch12[start:stop][j]
                        }
                    }
                )
        elif Board == "B0":
            ## PCB L6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L2",
                        "PCB" : "L6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch5[start:stop][j],
                        "Current" : i_ch5[start:stop][j]
                    }
                }
            )
            ## PCB R6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L2",
                        "PCB" : "R6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch4[start:stop][j],
                        "Current" : i_ch4[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'old':
                ## PCB L7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L2",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch13[start:stop][j],
                            "Current" : i_ch13[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L2",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch12[start:stop][j],
                            "Current" : i_ch12[start:stop][j]
                        }
                    }
                )
        ################### L3 #####################
        if Board == "B1":
            ## PCB L8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L3",
                        "PCB" : "L8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch3[start:stop][j],
                        "Current" : i_ch3[start:stop][j]
                    }
                }
            )
            ## PCB R8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L3",
                        "PCB" : "R8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch2[start:stop][j],
                        "Current" : i_ch2[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'new':
                ## PCB L7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L3",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch11[start:stop][j],
                            "Current" : i_ch11[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L3",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch10[start:stop][j],
                            "Current" : i_ch10[start:stop][j]
                        }
                    }
                )
        elif Board == "B0":
            ## PCB L6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L3",
                        "PCB" : "L6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch3[start:stop][j],
                        "Current" : i_ch3[start:stop][j]
                    }
                }
            )
            ## PCB R6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L3",
                        "PCB" : "R6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch2[start:stop][j],
                        "Current" : i_ch2[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'old':
                ## PCB L7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L3",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch11[start:stop][j],
                            "Current" : i_ch11[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L3",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch10[start:stop][j],
                            "Current" : i_ch10[start:stop][j]
                        }
                    }
                )
        ################### L4 #####################
        if Board == "B1":
            ## PCB L8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L4",
                        "PCB" : "L8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch1[start:stop][j],
                        "Current" : i_ch1[start:stop][j]
                    }
                }
            )
            ## PCB R8  B1
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L4",
                        "PCB" : "R8"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch0[start:stop][j],
                        "Current" : i_ch0[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'new':
                ## PCB L7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L4",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch9[start:stop][j],
                            "Current" : i_ch9[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B1
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L4",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch8[start:stop][j],
                            "Current" : i_ch8[start:stop][j]
                        }
                    }
                )
        elif Board == "B0":
            ## PCB L6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L4",
                        "PCB" : "L6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch1[start:stop][j],
                        "Current" : i_ch1[start:stop][j]
                    }
                }
            )
            ## PCB R6  B0
            influxdbContainer.append(
                {
                    "measurement": "HV_COSMIC_STAND",
                    "tags": {
                        "Module" : ModuleName,
                        "Layer" : "L4",
                        "PCB" : "R6"
                    },
                    "time": Time[start:stop][j],
                    "fields": {
                        "Voltage" : v_ch0[start:stop][j],
                        "Current" : i_ch0[start:stop][j]
                    }
                }
            )
            if connectStrategy == 'old':
                ## PCB L7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L4",
                            "PCB" : "L7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch9[start:stop][j],
                            "Current" : i_ch9[start:stop][j]
                        }
                    }
                )
                ## PCB R7  B0
                influxdbContainer.append(
                    {
                        "measurement": "HV_COSMIC_STAND",
                        "tags": {
                            "Module" : ModuleName,
                            "Layer" : "L4",
                            "PCB" : "R7"
                        },
                        "time": Time[start:stop][j],
                        "fields": {
                            "Voltage" : v_ch8[start:stop][j],
                            "Current" : i_ch8[start:stop][j]
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