(ns hs4clj.core-test
  (:require [clojure.test :refer :all]
            [midje.sweet :refer :all]
            [clojure.java.jdbc :as jdbc]
            [hs4clj.core :as hs4clj]))

; Database set up and teardown
; Note : for these tests to work you will need a mysql that is set up for
; handler socket.
; For OSX I used teh MariaDB installer
; http://www.cutedgesystems.com/weblog/index.php?entry=/Technology/MariaDBOnMountainLion.txt
; For Linux there is MariaDB, Percona (probably in your distro)
; or download Tokuteks MariaDB
; http://www.tokutek.com/resources/support/gadownloads/
; You will need to set up your mysql and my.conf after installation, see
; http://www.percona.com/doc/percona-server/5.5/performance/handlersocket.html

(def ^:private db {:classname "com.mysql.jdbc.Driver"
                   :subprotocol "mysql"
                   :subname "//localhost:3306/mytest"
                   :user "root"
                   :password ""})

(defn- reset-database!
  []
  (jdbc/db-do-commands db
                       true
                       "drop table if exists test_cats;"
                       (str "create table test_cats("
                            "id int primary key not null auto_increment"
                            ",name varchar(100) not null"
                            ",weight decimal(5,2) not null"
                            ",breed_id int not null"
                            ",date_of_birth datetime not null"
                            ",date_of_death datetime);"
                            )
                       (str "insert into test_cats("
                            "name,weight,breed_id,date_of_birth,date_of_death) "
                            "values "
                            "('Spider', 12.3, 1, '2001-01-24', '2012-06-30')"
                            ",('Tuna', 15.3, 2, '2001-01-24', NULL)"
                            ",('Tiki', 7.3, 3, '2013-01-15', NULL)"
                            ",('Tesla', 7.1, 3, '2013-02-15', NULL);"))
  )

; set up Handler Socket Connection
(def ^:private client (atom nil))

(defn- open-client
  []
  (reset! client
          (hs4clj/open-client :host "localhost" :pool-size 1 :port 9999)))


(defn- shutdown-client
  []
  (when @client (.shutdown @client)))

(background (before :facts (do (shutdown-client)
                               (reset-database!)
                               (open-client)))
            (after :facts (shutdown-client)))

; Database records as clojure data
(def cats
  {:spider {:id "1"
            :name "Spider"
            :weight "12.30"
            :breed_id "1"
            :date_of_birth "2001-01-24 00:00:00"
            :date_of_death "2012-06-30 00:00:00"}
   :tuna {:id "2"
          :name "Tuna"
          :weight "15.30"
          :breed_id "2"
          :date_of_birth "2001-01-24 00:00:00"
          :date_of_death nil}

   :tiki {:id "3"
          :name "Tiki"
          :weight "7.30"
          :breed_id "3"
          :date_of_birth "2013-01-15 00:00:00"
          :date_of_death nil}
   :tesla {:id "4"
           :name "Tesla"
           :weight "7.10"
           :breed_id "3"
           :date_of_birth "2013-02-15 00:00:00"
           :date_of_death nil}})

(facts "about running a query against the the single primary key"
  (let [columns [:id :name :weight :breed_id :date_of_birth :date_of_death]
        session #(hs4clj/open-session :client @client
                                      :db :mytest
                                      :table :test_cats
                                      :index :PRIMARY
                                      :columns columns)]
    (fact "will return a single equality match with the default arguments"
      (hs4clj/query :session (session)
                    :index-values [1]) => [(:spider cats)])
    (fact "will return multiple matches with additional arguments"
      (hs4clj/query :session (session)
                    :operator >
                    :limit 100
                    :index-values [1]) => (map cats [:tuna :tiki :tesla]))
    (fact "will use limits and offsets"
      (hs4clj/query :session (session)
                    :operator >
                    :limit 1
                    :offset 1
                    :index-values [1]) => [(:tiki cats)])

    (fact "alternate with-session syntax"
      (hs4clj/with-session (session)
         (hs4clj/query :operator >
                       :limit 100
                       :index-values 0)
          => (map cats [:spider :tuna :tiki :tesla])))
    (fact "with-session wrapping a map function"
      (hs4clj/with-session (session)
        (let [find-fn #(first (hs4clj/query :index-values %))]
          (map find-fn [1 2 3 4]) => (map cats [:spider :tuna :tiki :tesla]))))))
