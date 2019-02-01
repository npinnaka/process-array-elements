(ns process-array-elements.core
  (:require [process-array-elements.utils :as util]
            [flambo.sql :as sql]
            [flambo.api :as api])
  (:gen-class))

(defn generic-row->vec[row]
  (let [n (.length row)]
    (loop [i 0
           v (transient [])]
      (if (< i n)
        (recur (inc i) (conj! v (.get row i)))
        (persistent! v)))))

(defn convert-to-map[schema collection flag]
  (loop [input-collection collection
         v                (transient [])]
    (if (.isEmpty input-collection)
      (persistent! v)
      (recur (.tail input-collection)
        (conj! v
               (let [inner-collection (zipmap schema (generic-row->vec (.head collection)))]
                 (if (true? flag)
                   (merge inner-collection
                          {"stock_dividends" (convert-to-map
                                              util/stock-schema-vec
                                              (get inner-collection "stock_dividends") false)})
                   inner-collection)))))))

(defn do-merge[tranx]
  (merge tranx {"stocks" (convert-to-map util/stock-schema-vec (get tranx "stocks") true)}))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (util/build-spark-local-context "welcome")
  (let [df         (sql/json-file util/sql-context "resources/data.json")
        parent-rdd (util/get-cached-rdd df)
        final-rdd  (api/map parent-rdd
                            (api/fn [trx]
                              (do-merge trx)))]

    (sql/print-schema df)
    (println "parent rdd is " (.collect final-rdd))))

;(-main)
