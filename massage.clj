(ns massage
    (:require
     [clojure.java.io :as io]
     [clojure.string :as sring]
     [java-time :as t]
     )
    (:import
     (com.influxdb.client.domain WritePrecision)
     (com.influxdb.client WriteApi InfluxDBClientFactory))
    )

(defn concat-files [dir starts-with outfile]
  "Concatenate files in the dir with names that starts-with the provided string to outfile.   
   Drop the first two lines of each file after the first file.
   Trim the whitespace of any subsequent lines."
  (let [drop-counts (cons 0 (repeat 2))
        counts-n-files (->> (io/file dir)
                            .listFiles
                            (filter #(string/starts-with? (.getName %) starts-with))
                            (sort-by #(.getName %))
                            (interleave drop-counts)
                            (partition 2))
        ]
    (doseq [[drop-count f] counts-n-files]
      (with-open [rdr (io/reader f)
                  wtr (io/writer outfile :append true)]
        (doseq [l (drop drop-count (line-seq rdr))]
          (let [line (-> l
                         (string/trim)
                         (string/replace ";" ",")
                         (str "\n"))]
            (.append wtr line))
          )
        )
      )
    )
  )
  

(defn influx-field [k v]
  (str k "=" v))

(defn influx-tag [k v]
  (str "," k "=" v))

(defn to-ts [date-time]
  ;; Expects time in the format 2020/02/01,00:05:44  
  (-> (t/local-date-time "yyyy/MM/dd,HH:mm:ss" date-time)
      (t/zoned-date-time (zone-id "America/Los_Angeles"))
      (t/offset-date-time)
      (t/to-millis-from-epoch)))

(comment

  (let [solarrig-token "MEA5zvegPKidKMcei3Qbkmd1vXSsRavZKrLaxEu-iRpFYu7qsP0_NuqdzNgj7_LoJ6DKGSi8Wb3mesJcTicJWw=="
        url "http://localhost:8086"
        org "solar-rig"
        bucket "solar-rig-bucket"
        precision WritePrecision/NS
        metric-writer (fn [] (let [cf (InfluxDBClientFactory/create url (char-array token))]
                               (.getWriteApiBlocking cf)))
        append-line (fn [influx-client line] (.writeRecord influx-client bucket org precision line))        
        ]
    (with-open [wtr (metric-writer)]
      (append-line wtr (str "garage-invertor"
                            (influx-tag "tagKey" "tagValue")
                            (influx-field "Gen Status" 49725)
                            (to-ts "2000/02/01,00:05:44")))
      )
    )
  
  ;; Date,Time,Gen status,Gen freq,Gen volt,Grid status,Grid freq,Grid volt,Input current,Input VA,Input watt,Output freq,Output volt,Output current,Output VA,Output watt,Battery volt,Battery current,External current,Battery temperature(C),Transformer temperature(C),Bus bar temperature(C),Heat sink temperature(C),Fan speed,Mode,Error code,Charge stage,Event,
  ;; 2000/02/01,00:05:44,49725,000.00,000.43,49727,000.00,000.24,<00.10,<0012,<0012,000.00,000.27,<00.10,<0012,<0012,13.382,0000.0,0000.0,0025.0,0007.9,0010.9,0010.6,0,0,00000,0,

  (def dir "/Users/mthomas/Dev/solar-rig-data/data")
  (concat-files dir "03" "mar-2020.csv")
  (concat-files dir "02" "feb-2020.csv")
  (concat-files dir "01" "jan-2020.csv"))
