# solar-rig
Scripts and odds n ends to manipulate data from my solar inverter rig

## Loading data from a SAMLEX EVO-2212

Use these steps to load the event data from a EVO-2212 where the
optional Remote Control EVO-RC has been installed. The EVO-RC has an
SD Card slot that accepts a 32 Gb (or less) card. Event data aka "the
datalog" is written to an SD Card (FAT 16/FAT 32) in the following
format.

```
EVO-2212
Date;Time;Gen status;Gen freq;Gen volt;Grid status;Grid freq;Grid volt;Input current;Input VA;Input watt;Output freq;Output volt;Output current;Output VA;Output watt;Battery volt;Battery current;External current;Battery temperature(C);Transformer temperature(C);Bus bar temperature(C);Heat sink temperature(C);Fan speed;Mode;Error code;Charge stage;Event;
2022/06/05;08:26:47;49725;000.00;000.31;16384;059.94;119.15;<00.10;<0012;<0012;060.00;120.00;<00.10;<0012;<0012;13.197;0000.5;0000.0;0025.0;0039.2;0033.7;0030.5;0;1;00000;0;
```

1. Copy data from the SD card to your computer. Often the card is mounted as 'NO NAME' on your Mac.
   `cp -Rn /Volumes/NO\ NAME/DATALOG $PWD`

1. Ensure Clojure is installed, I use `brew` in a Mac for this
   `brew install clojure`

1. Ensure that Docker is installed and start an InfluxDB process
   `./influx.sh`

1. Open a browser to http://127.0.0.1:8086/
   For example on MacOS rrun `open http://127.0.0.1:8086/`

1. If it is a new instance of Influx, create
   1. a user and password e.g. `solarrig`
   1. an Influx organization e.g. `solar-org`
   1. an Influx bucket for the data e.g. `garage-samlex-evo-2012`
   1. an API token allowing read/write to the bucket

1. Modify the message.clj with the client api token, org
   
1. Run the load-rig-data alias defined in the deps.edn
   `clojure -X:load-rig-data :data-dir '"/Users/mthomas/Dev/solar-rig-data/DATALOG"'`
