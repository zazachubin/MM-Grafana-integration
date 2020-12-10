This package is created for ATLAS NSW Micromegas detectors production in JINR Dubna Russia,
which contains different monitoring system (High Voltage, Dewpoint, Room environment) with Grafana integration.

///////////////////////// Python libraries /////////////////////////
#################### install pysnmp libraries ######################
python3 -m pip install pysnmp
############## install terminal table visualization ################
python3 -m pip install texttable
######################### install InfluxDb #########################
python3 -m pip install influxdb
python3 -m pip install influxdb-client'
////////////////////////////////////////////////////////////////////

The package includes scripts for:
    * Real time monitoring
    * Data log uploader to InfluxDb

####################### Real time monitoring ######################
Hardware:
    * High voltage power supplay WIENER Mpod-Minicrate LV / HV:
            Modules: 2x EHS F2 40p. 16ch | 4kV | 100uA

    * Dewpoint sensor : DMT143 with PicoLog 1000 series USB data logger.

    * Room environment monitoring: BME280 with Arduino nano.

Software:
    Server requirements
        * Python
        * InfulxDb
        * Grafana

    * Run high voltage power supplay data monitoring:
        python3 HV_server.py --M <ModuleName>

        optional arguments:
        -h, --help              show this help message and exit
        --M "test"              Module name
        --delay "1"             HV device reading delay
        --TermPrint "off"       Terminal print data on/off
        --log "on"              log on/off
        --database "on"         database on/off
        --hvIp "192.168.0.250"  HV IP Address
        --avgTime "1"           Averaging time
        --dbHost "bes3.jinr.ru" Database host address
        --dbName "MM_Dubna"     Database name
        --dbUserName "MM_Dubna" Database user name
        --dbPasswd "******"     Database password
        --dbPort "8086"         Database port number

    * Run dewpoint monitoring:
    python3 Dewpoint_server.py

################## Data log uploader to InfluxDb ##################
