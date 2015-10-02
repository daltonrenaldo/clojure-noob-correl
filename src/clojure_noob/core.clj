(ns clojure-noob.core
  (:gen-class)
  (:require [clj-time.jdbc]))

(require '[clj-postgresql.core :as psql])
(require '[clojure.java.jdbc :as jdbc])
(require '[clj-time.core :as timex])
(require '[clj-time.coerce :as coerce])
(require '[clj-time.periodic :as time-period])
(require '[clj-time.local :as local])

(use '(incanter core stats charts io))

(def db (psql/spec :host "localhost:5432" :user "root" :dbname "macro_monitor_dev" :password "password"))

(defn process [a e]
  (let [name (:name e) value (dissoc e :name)]
    (if (get a name)
      (assoc a name (conj (get a name) value))
      (assoc a name (list value))
    )
  )
)

(defn quarters
  "Return a lazy sequence of DateTime's from start to end, incremented
  by 'step' units of time."
  [start end]
  (def step (timex/months 3))
  (let [inf-range (time-period/periodic-seq start step)
        below-end? (fn [t] (timex/within? (timex/interval start end)
                                         t))]
    (take-while below-end? inf-range))
)

(defn value_from [dataset datetime]
  (def a (first dataset))
  (def r (rest dataset))
  (def date (coerce/to-local-date datetime))

  (if (timex/equal? (coerce/to-local-date (get a :date)) date)
    (:value a)
    (if (empty? r)
      nil
      (value_from r date)
    )
  )
)


(defn correlate [dataset1 dataset2 dates]
  "correlates two datasets"
  (loop [d1 (first dates) remaining_dates (rest dates) list1 (vector) list2 (vector)]
    (if d1
      (do
        (def v1 (value_from dataset1 d1))
        (def v2 (value_from dataset2 d1))

        (if (some? v1)
          (if (some? v2)
            (do
              (recur (first remaining_dates) (rest remaining_dates) (conj list1 v1) (conj list2 v2))
            )
            (recur (first remaining_dates) (rest remaining_dates) list1 list2)
          )
          (recur (first remaining_dates) (rest remaining_dates) list1 list2)
        )
      )
      (do
        (if (and (not (empty? list1)) (not (empty? list2)))
          (incanter.stats/correlation list1 list2)
        )
      )
    )
  )
)

(defn run [ & args]
  ; (def start (timex/now))
  (def result (jdbc/query db ["SELECT series.name, data_points.* FROM series
    LEFT OUTER JOIN data_points ON data_points.series_id = series.id
    WHERE (data_points.date BETWEEN '2010-01-01' AND '2011-01-01'
    )"]))

  (def result-processed (reduce process {} result))

  (def series_list (into () result-processed))
  (def list [])

  (loop [series1 (first series_list) remaing_datasets (rest series_list)]
    (def name1 (first series1))
    (def dataset1 (last series1))

    (loop [series2 (first remaing_datasets) remaing_datasets2 (rest remaing_datasets)]
      (def name2 (first series2))
      (def dataset2 (last series2))

      (def list
        (conj list
          (correlate dataset1 dataset2 (quarters (timex/date-time 2010 1 1) (timex/date-time 2011 1 1)) )
        )
      )

      (if (> (count remaing_datasets2) 0)
        (recur (first remaing_datasets2) (rest remaing_datasets2))
      )
    )

    (if (> (count remaing_datasets) 1)
      (recur (first remaing_datasets) (rest remaing_datasets))
      (println (count list))
    )
  )
)

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (time (run))
)
