(defproject arctype/steelyx "0.2.0-SNAPSHOT"
  :dependencies 
  [[org.clojure/clojure "1.10.0"]
   [arctype/service "1.0.0"]
   [arctype/service.curator "0.1.0-SNAPSHOT"
    :exclusions [arctype/service]]
   [arctype/service.quartzite "0.1.1-SNAPSHOT"
    :exclusions [arctype/service]]
   [metosin/compojure-api "2.0.0-alpha29"]
   [org.onyxplatform/onyx "0.14.4-arctype"]])
