(ns massage
    (:require
     [clojure.java.io :as io]
     [clojure.string :as string]
     [java-time :as t]
     )
    (:import
     (com.influxdb.client.write Point)
     (com.influxdb.client.domain WritePrecision)
     (com.influxdb.client InfluxDBClientFactory)
     )
    )

(defn concat-files
  "Concatenate files in the dir with names that starts-with the provided string to outfile.   
   Drop the first two lines of each file after the first file.
   Trim the whitespace of any subsequent lines."
  [dir starts-with outfile]
  
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

(defn format-column-name
  "Format column names fields lower-case each name, remove parens statements and replace whitespace characters with an underscore"
  [column-name]
  (->
   column-name
   (string/lower-case)
   (string/replace #"\s+" "_")
   (string/replace #"\(\w+\)" "")
   )
  )

(defn influx-field [k v]
  (str k "=" v))

(defn influx-tag [k v]
  (str k "=\"" v "\""))

(defn to-ts [date time]
  ;; Expects time in the format 2020/02/01 and 00:05:44  
  (-> (t/local-date-time "yyyy/MM/dd HH:mm:ss" (str date " " time))
      (t/zoned-date-time (t/zone-id "America/Los_Angeles"))
      (t/offset-date-time)      
      (t/to-millis-from-epoch)))

(defn to-instant [date time]
  ;; Expects time in the format 2020/02/01 and 00:05:44  
  (-> (t/local-date-time "yyyy/MM/dd HH:mm:ss" (str date " " time))
      (t/zoned-date-time (t/zone-id "America/Los_Angeles"))
      (t/offset-date-time)
      (t/instant)))

(defn floaty [str-float]
  (try
    (Float/parseFloat str-float)
    (catch Exception e
      (try
        (Float/parseFloat (string/replace str-float #"<" ""))
        (catch Exception e str-float)))))

(defn influx-fields-str [field-key-map]
  (apply str (interpose "," (for [[k v] field-key-map :when (float? v)] (influx-field k v))))
  )

(defn influx-point-fields [point field-key-map]
  (reduce #(.addField %1 (first %2) (second %2)) point (seq field-key-map)))

(defn influxify
  "Take a sequence of lines in csv format. The first line is the column
  headers and the remainder is the associated data for each
  column. Convert lines to the Influx Point so that it can be
  imported into an InfluxDB instance"
  [measurement-name csv-line-seq]
  (let [column-names (map format-column-name (string/split (first csv-line-seq) #","))
        rows (map #(string/split % #",") (drop 1 csv-line-seq))
        cells (for [row rows]
                (into {} (map #(vector %1 (floaty %2)) column-names row)))
        with-instant (map #(assoc % "timestamp" (to-instant (% "date")  (% "time"))) cells)
        wo-date-time (map #(dissoc % "date" "time") with-instant)
        ]
    
    (map #(->
           (new Point measurement-name)
           (.addTag "location" "garage")
           (.addTag "battery" "Relion RB100")
           (.addTag "load1" "garage door")
           (.addTag "load2" "PurpleAir sensor")
           (influx-point-fields (dissoc % "timestamp"))
           (.time (% "timestamp") WritePrecision/MS))
         wo-date-time)
    ;; (map #(str measurement-name ","
    ;;            (influx-tag "location" "garage") ","
    ;;            (influx-tag "battery" "Relion RB100") ","
    ;;            (influx-tag "load1" "garage door") ","
    ;;            (influx-tag "load2" "purple air sensor") " "
    ;;            (influx-fields %) " "
    ;;            (% "timestamp")) wo-date-time) 
    )
  )
  

(comment
  
  (str "," "foo" "=" "bar", " ", "x")
  ;; user:solarrig
  ;; password:the usual
  (let [solarrig-token "q919gL53Udy569RVtLY8K2E6efrBjGuxa17RV-ylRI40puYJLFw_EkkrPyP_TuNkof6vwmAIWpSzeOfcZDz3-Q=="
        url "http://localhost:8086"
        org "solar-rigs"
        bucket "samlex-evo-2012"
        precision WritePrecision/MS]
    (with-open [ic (InfluxDBClientFactory/create url (char-array solarrig-token))]
      (let [wtr (.getWriteApiBlocking ic)
            append-line (fn [line] (.writeRecord wtr bucket org precision line))]
        (append-line (str "invertor-sample"
                          (influx-tag "tagKey" "tagValue") " "
                          (influx-field "genStatus" (rand 100)) " "
                          (to-ts "2022/05/01" "10:47:02"))))))
    
  (let [solarrig-token "q919gL53Udy569RVtLY8K2E6efrBjGuxa17RV-ylRI40puYJLFw_EkkrPyP_TuNkof6vwmAIWpSzeOfcZDz3-Q=="
        url "http://localhost:8086"
        org "solar-rigs"
        bucket "samlex-evo-2012"]
    (with-open [ic (InfluxDBClientFactory/create url (char-array solarrig-token))
                rdr (io/reader "/Users/mthomas/Dev/solar-rig-data/test.csv")]
      (let [wtr (.getWriteApiBlocking ic)
            write-point (fn [point]
                          (println "Writing point" (.toLineProtocol point))
                          (.writePoint wtr bucket org point)
                          )]
        (doseq [point (influxify "a-inverter-sample" (line-seq rdr))]
          (write-point point))
        )
      )
    )

  (def dir "/Users/mthomas/Dev/solar-rig-data/data")
  (concat-files dir "03" "mar-2022.csv")
  (concat-files dir "02" "feb-2022.csv")
  (concat-files dir "01" "jan-2022.csv")

  (concat-files dir "1027040" "test.csv")

  (def lines ["Date,Time,Gen status,Gen freq,Gen volt,Grid status,Grid freq,Grid volt,Input current,Input VA,Input watt,Output freq,Output volt,Output current,Output VA,Output watt,Battery volt,Battery current,External current,Battery temperature(C),Transformer temperature(C),Bus bar temperature(C),Heat sink temperature(C),Fan speed,Mode,Error code,Charge stage,Event,",
              "2021/10/27,04:08:18,49725,000.00,000.39,49727,000.00,000.31,<00.10,<0012,<0012,060.00,120.12,<00.10,<0012,<0012,11.141,0000.0,0000.0,0025.0,0023.0,0021.0,0018.5,0,1,00000,0,",
              "2021/10/27,04:08:19,49725,000.00,000.39,49723,000.00,000.31,<00.10,<0012,<0012,060.00,120.11,<00.10,<0012,<0012,11.141,0000.0,0000.0,0025.0,0023.0,0021.0,0018.5,0,1,00000,0,"])

  (influxify "invertor-sample" (seq lines))

  
    
  (with-open [rdr (io/reader "/Users/mthomas/Dev/solar-rig-data/test.csv")]
    rdr
    )
  
  )
