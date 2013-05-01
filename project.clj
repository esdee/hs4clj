(defproject hs4clj "0.1.0-SNAPSHOT"
  :description "Clojure wrapper around hs4j providing java handler socket client"
  :url "https://github.com/esdee/hs4clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.0-alpha1"]
                 [mysql/mysql-connector-java "5.1.6"]
                 [log4j/log4j "1.2.8"]
                 [org.slf4j/slf4j-api "1.5.6"]
                 [org.slf4j/slf4j-log4j12 "1.3.0"]])
