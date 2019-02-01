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

;; processing json in the following format, tp clojure map
(comment

  {
    "id": 1,
    "stocks": [
                {
                  "name": "Apple",
                  "price": 105.00,
                  "quantity": 50,
                  "stock_dividends": [
                                       {
                                         "amount": 0.53,
                                         "date": "20190213"
                                         },
                                       {
                                         "amount": 0.45,
                                         "date": "20181113"
                                         }
                                       ]
                  },
                {
                  "name": "Ford",
                  "price": 8.05,
                  "quantity": 150,
                  "stock_dividends": [
                                       {
                                         "amount": 0.05,
                                         "date": "20190313"
                                         },
                                       {
                                         "amount": 0.05,
                                         "date": "20181213"
                                         }
                                       ]
                  }
                ]
    }



  )
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
