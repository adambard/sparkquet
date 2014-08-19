(defproject sparkquet "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]

                 ; Spark wrapper
                 [yieldbot/flambo "0.3.2"]

                 ; Still need spark & hadoop (must pick specific client)
                 [org.apache.spark/spark-core_2.10 "1.0.1"]
                 [org.apache.hadoop/hadoop-client "2.4.1"
                  :exclusions [javax.servlet/servlet-api]] ; Conflicts with spark's

                 ; Parquet stuff
                 [com.twitter/parquet-common "1.6.0rc1"]
                 [com.twitter/parquet-encoding "1.6.0rc1"]
                 [com.twitter/parquet-column "1.6.0rc1"]
                 [com.twitter/parquet-hadoop "1.6.0rc1"
                  :exclusions [javax.servlet/servlet-api] ]
                 [com.twitter/parquet-protobuf "1.6.0rc1"
                  :exclusions [javax.servlet/servlet-api commons-lang]]

                 ; And, of course, protobufs
                 [com.google.protobuf/protobuf-java "2.5.0"]
                 ]
  :java-source-paths ["src/java"]
  :source-paths ["src/clj"]
  :plugins [[lein-protobuf "0.4.1"]]
  )
