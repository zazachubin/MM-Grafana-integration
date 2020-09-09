This package is created for ATLAS NSW Micromegas detectors production in JINR Dubna Russia,
which contains different monitoring system (High Voltage, Dewpoint, Room environment) with Grafana integration.

Hardware:
    * High voltage power supplay WIENER Mpod-Minicrate LV / HV:
            Modules: 2x EHS F2 40p. 16ch | 4kV | 100uA

    * Dewpoint sensor : DMT143 with PicoLog 1000 series USB data logger.

    * Room environment monitoring: BME280 with Arduino nano.

Software:
    Server requirements
        * MQTT broker
        * InfulxDb
        * Grafana

    All software are organized by MQTT system which colects all data and push to InfluxDb.
    Data is visualized by Grafana.



HV reading and publishing on MQTT broker
run : 
Dewpoint reading and publishing on MQTT broker
