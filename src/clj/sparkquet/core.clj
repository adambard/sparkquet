(ns sparkquet.core
  (:require [flambo.conf :as conf]
            [flambo.api :as f])
  (:import
    [parquet.hadoop ParquetOutputFormat ParquetInputFormat]
    [parquet.proto ProtoParquetOutputFormat ProtoParquetInputFormat ProtoWriteSupport ProtoReadSupport]
    org.apache.hadoop.mapreduce.Job
    sparkquet.Document$MyDocument ; Import our protobuf
    sparkquet.Document$MyDocument$Category ; and our enum
    sparkquet.OnlyStuff
    parquet.column.ColumnReader
    [parquet.filter RecordFilter ColumnRecordFilter UnboundRecordFilter ColumnPredicates]
    ))



(defn make-protobuf
  "Helper function to make a protobuf from a hashmap. You could
  also use something like clojure-protobuf:
  https://github.com/ninjudd/clojure-protobuf"
  [data]
  (let [builder (Document$MyDocument/newBuilder)]
    (doto builder
      (.setId (:id data))
      (.setName (:name data))
      (.setDescription (:description data))
      (.setCategory (:category data))
      (.setCreated (:created data)))
    (.build builder)))
 
(defn produce-my-protobufs
  "This function serves as a generic source of protobufs. You can replace
  this with whatever you like. Perhaps you have a .csv file that you can
  open with f/text-file and map to a protobuf? Whatever you like."
  [sc]
  (f/parallelize
    sc
    (map make-protobuf [
      {:id "1" :name "Thing 1" :description "This is a thing" :category Document$MyDocument$Category/THINGS :created (System/currentTimeMillis)}
      {:id "2" :name "Thing 2" :description "This is a thing" :category Document$MyDocument$Category/THINGS :created (System/currentTimeMillis)}
      {:id "3" :name "Crap 1" :description "This is some crap" :category Document$MyDocument$Category/CRAP :created (System/currentTimeMillis)}
      {:id "4" :name "Stuff 1" :description "This is stuff" :category Document$MyDocument$Category/STUFF :created (System/currentTimeMillis)}
      {:id "5" :name "Stuff 2" :description "This is stuff" :category Document$MyDocument$Category/STUFF :created (System/currentTimeMillis)}
      {:id "6" :name "Stuff 3" :description "This is stuff" :category Document$MyDocument$Category/STUFF :created (System/currentTimeMillis)}
      {:id "7" :name "Stuff 4" :description "This is stuff" :category Document$MyDocument$Category/STUFF :created (System/currentTimeMillis)}])))


(defn write-protobufs!
  "Use Spark's .saveAsNewAPIHadoopFile to write a your protobufs."
  [rdd job outfilepath]
  (-> rdd
      (f/map-to-pair (f/fn [buf] [nil buf])) ; We need to have a PairRDD to call .saveAsNewAPIHadoopFile on
      (.saveAsNewAPIHadoopFile
        outfilepath                 ; Can (probably should) be an hdfs:// url
        Void                        ; We don't have a key class, just some protobufs
        Document$MyDocument         ; Would be a static import + .class in java
        ParquetOutputFormat         ; Use the ParquetOutputFormat
        (.getConfiguration job))))  ; Protobuf things are present on the job config.))

(defn read-protobufs
  "Use Spark's .newAPIHadoopFile to load your protobufs"
  [sc job infilepath]
  (->
    (.newAPIHadoopFile sc
      infilepath            ; Or hdfs:// url
      ParquetInputFormat
      Void                  ; Void key (.newAPIHadoopFile always returns (k,v) pair rdds)
      Document$MyDocument   ; Protobuf class for value
      (. job getConfiguration))

    (f/map (f/fn [tup] (._2 tup))))) ; Strip void keys from our pair data.

(defn get-job
  "Important initializers for Parquet Protobuf support. Updates a job's configuration"
  []
  (let [job (Job.)]

    ; You need to set the read support and write support classes
    (ParquetOutputFormat/setWriteSupportClass job ProtoWriteSupport)
    (ParquetInputFormat/setReadSupportClass job ProtoReadSupport)

    ; You also need to tell the writer your protobuf class (reader doesn't need it)
    (ProtoParquetOutputFormat/setProtobufClass job Document$MyDocument)

    job))

(defn -main []
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[4]") ; Run locally with 4 workers
                 (conf/app-name "protobuftest"))
        sc (f/spark-context conf) ; Create a spark context
        job (get-job) ; Create a Hadoop job to hold configuration 
        path "hdfs://localhost:9000/user/protobuftest2"
        ]

    ; First, we can write our protobufs
    (-> sc
        (produce-my-protobufs) ; Get your Protobuf RDD
        (write-protobufs! job path))

    ; Now, we can read them back
    (-> sc
        (read-protobufs job path)
        (f/collect)
        (first)
        (.getId)
        )

    ; You can also add a Parquet-level filter on your job to massively improve performance
    ; when running queries that can be easily pared down.
    (ParquetInputFormat/setUnboundRecordFilter job
                                               (reify
                                                 UnboundRecordFilter
                                                 (bind [this readers]
                                                   (-> (ColumnRecordFilter/column
                                                         "category"
                                                         (ColumnPredicates/equalTo Document$MyDocument$Category/STUFF))
                                                       (.bind readers)))))

    (-> sc
        (read-protobufs job path)
        (f/collect)) ; There should only be the 4 items now.

    ; If you like, you can set a *projection* on your job. This will read a subset of your
    ; fields for efficiency. Here's what you might do if you just needed names filtered by category:
    (ProtoParquetInputFormat/setRequestedProjection job "message MyDocument { required binary name; required binary category; }")

    (-> sc
        (read-protobufs job path)
        (f/map (f/fn [buf] (.getName buf)))
        (f/collect)) ; Remember, the record filter is still applied.

    ))


; Defs for REPL usage
(comment 
  (def conf (-> (conf/spark-conf)
                (conf/master "local[4]") ; Run locally with 4 workers
                (conf/app-name "protobuftest")))
  (def sc (f/spark-context conf))   ; Create a spark context
  (def job (get-job)) ; Create a Hadoop job to hold configuration 
  (def path "hdfs://localhost:9000/user/protobuftest4" )
  )
