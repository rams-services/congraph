(defproject congraph "0.1.6"
  :description "a db connection management library that copies heavly from conman but allows for multiple type of connections"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[com.layerware/hugsql-core "0.5.1"]
                 [com.layerware/hugsql-adapter-next-jdbc "0.5.1"]
                 [com.carouselapps/to-jdbc-uri "0.5.0"]
                 [seancorfield/next.jdbc "1.1.613"]
                 [hikari-cp "2.13.0"]
                 [org.clojure/clojure "1.11.1"]
                 [net.clojars.rams-services/hugcypher "0.1.3"]]
  :profiles
  {:dev {:dependencies [[com.h2database/h2 "1.4.200"]
                        [mount "0.1.16"]]}})
