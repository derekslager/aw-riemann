(ns aw-riemann.client
  (:require [clj-http.client :as http]
            [clj-time.core :as time]
            [clj-time.coerce :as c]
            [clj-time.format :as f]
            [clojure.tools.cli :as cli]
            [environ.core :refer [env]]
            [riemann.client :as riemann]
            [clojure.string :as str]))

(defn api-request [endpoint & [{:keys [method] :as opts}]]
  ((if (= :post method) http/post http/get)
   (str (env :api-url "http://localhost:5600/api/") endpoint)
   (-> (or opts {})
       (assoc :as :json))))

(defn buckets []
  (->> (api-request "0/buckets/")
       :body
       (map second)))

(defn bucket-prefix [bucket-id]
  (str "0/buckets/"
       (http/url-encode-illegal-characters bucket-id)))

(defn bucket-metadata [bucket-id]
  (api-request (bucket-prefix bucket-id)))

(defn parse-aw-timestamp [timestamp]
  (f/parse timestamp))

(defn aw-event->riemann [{:keys [hostname type client] :as bucket}
                         {:keys [timestamp duration data] :as event}]
  (merge
   ;; `data` is arbitrary, merge riemann data last in case a keyword
   ;; clashes -- index these as needed in riemann config
   data
   {:host hostname
    :service (str "activitywatch " type)
    :metric duration
    :time (/ (c/to-long (parse-aw-timestamp timestamp)) 1000)
    :ttl (* 60 60 24 1000)}))

(defn afk-bucket? [bucket]
  (= "aw-watcher-afk" (:client bucket)))

(def afk-filter
  (filter afk-bucket?))

(defn query [hours id merge-keys]
  (let [now (time/now)
        then (time/minus now (time/hours hours))
        buckets (buckets)
        afks (into [] afk-filter buckets)
        q [(str "afks = concat(" (str/join "," (map #(str "query_bucket(find_bucket('" (:id %) "'))") afks)) ");")
           (str "events = query_bucket(find_bucket('" id "'));")
           (str "events = filter_period_intersect(events, filter_keyvals(afks, 'status', ['not-afk']));")
           (str "events = merge_events_by_keys(events, [" (str/join "," (map #(str "'" % "'") merge-keys)) "]);")
           "RETURN = sort_by_timestamp(events);"]]

    (api-request
     "0/query/"
     {:method :post
      :form-params {:query q
                    :timeperiods [(str then "/" now)]}
      :content-type :json})))

(def client->merge-keys
  {"aw-watcher-window" ["app" "title"]
   "emacs-activity-watch" ["language" "project" "file"]
   "aw-watcher-web" ["url" "title" #_"audible" #_"incognito"]})

(defn collect-events [hours]
  (for [bucket (filter (complement afk-bucket?) (buckets))]
    (let [merge-keys (get client->merge-keys (:client bucket))]
      (if merge-keys
        (map (partial aw-event->riemann bucket)
             (-> (query hours (:id bucket) merge-keys) :body first))
        (println "Don't know how to merge " (:client bucket))))))
#_(collect-events 2)

(defn send-events [hours]
  (with-open [rc (riemann/tcp-client {:host (env :riemann-host "localhost")})]
    (doseq [events (collect-events hours) :when events]
      (doseq [batch (partition-all 100 events)]
        (let [batch-results
              (->> batch
                   (map #(riemann/send-event rc %))
                   doall
                   (map #(deref % 2000 {:ok false})))]
          (when-not (every? :ok batch-results)
            (throw (Exception. "Error sending batch.")))))
      (println "Successfully sent" (count events) "events"))))

;;; backfill 60 days
#_(send-events (* 24 60))
;;; backfill 2 days
#_(send-events (* 24 2))

(def cli-options
  ;; An option with a required argument
  [["-b" "--backfill DAYS" "Number of days to backfill"
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 %) "Must be a positive number"]]])

(defn -main [& args]
  (let [opts (cli/parse-opts args cli-options)]

    (if (:errors opts)
      (println (str/join ", " (:errors opts)))

      (do
        (when-let [backfill (-> opts :options :backfill)]
          (println (str "Backfilling " backfill " day(s)"))
          (send-events (* 24 backfill)))

        (let [c (select-keys env [:api-url :riemann-host])]
          (when (seq c)
            (println "Using configuration" c)))

        (while true
          (send-events 1)
          ;; 30m
          (Thread/sleep (* 1000 60 30)))))))
