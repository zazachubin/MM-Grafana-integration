#!/usr/bin/env python3
from influxdb import InfluxDBClient
import argparse
import csv

############################ Requirement ##############################
# Source Code: https://github.com/zazachubin/MM-Grafana-integration
# python3 -m pip install influxdb
# python3 -m pip install influxdb-client
################################ Run ##################################
# python3 dewpoint_log_uploader2db --file <path>/<filename>

def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        description='Dewpoint data uploader to database')
    parser.add_argument('--file', type=str, required=False,
                        default='',
                        help='File name')
    parser.add_argument('--w', type=int, required=False,
                        default=1000,
                        help='Data transfer window size to database')
    return parser.parse_args()

args = parse_args()
FileName = args.file
windowSize = args.w

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
dewpoint = []
RH = []

counter = 0
with open(FileName) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        if counter != 0:
            Time.append(row[0])
            dewpoint.append(float(row[1]))
            RH.append(float(row[2]))

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

        influxdbContainer.append(
        {
            "measurement": "Dewpoint_COSMIC_STAND",
            "tags": {
                "Sensor" : 'DMT143'
            },
            "time": Time[start:stop][j],
            "fields": {
                "Dewpoint" : dewpoint[start:stop][j]
            }
        }
        )

        influxdbContainer.append(
            {
                "measurement": "Dewpoint_COSMIC_STAND",
                "tags": {
                    "Sensor" : 'DMT143'
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Humidity" : RH[start:stop][j]
                }
            }
        )

        printProgressBar(j, len(Time[start:stop]), prefix = "Progress: {}/{} -> UTC-Time: {}".format(i, windowNumber, Time[start:stop][-1]), suffix = 'Complete', length = 25)

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
        influxdbContainer.append(
        {
            "measurement": "Dewpoint_COSMIC_STAND",
            "tags": {
                "Sensor" : 'DMT143'
            },
            "time": Time[start:stop][j],
            "fields": {
                "Dewpoint" : dewpoint[start:stop][j]
            }
        }
        )
        influxdbContainer.append(
            {
                "measurement": "Dewpoint_COSMIC_STAND",
                "tags": {
                    "Sensor" : 'DMT143'
                },
                "time": Time[start:stop][j],
                "fields": {
                    "Humidity" : RH[start:stop][j]
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