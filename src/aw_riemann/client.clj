(ns aw-riemann.client
  (:require [clj-http.client :as http]
            [clj-time.coerce :as c]
            [clj-time.format :as f]
            [environ.core :refer [env]]
            [riemann.client :as riemann]))

(defn api-request [endpoint & [opts]]
  (http/get (str (env :api-url "http://localhost:5600/api/") endpoint)
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

(defn export-bucket [bucket-id]
  (api-request (str (bucket-prefix bucket-id) "/export")))

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

(defn send-events [bucket-filter]
  (with-open [rc (riemann/tcp-client {:host (env :riemann-host "localhost")})]
    (doseq [bucket-id (->> (buckets) (map :id))
            :when (bucket-filter bucket-id)]
      (println "importing" bucket-id)
      (let [response (:body (export-bucket bucket-id))
            header (dissoc response :events)
            events (map (partial aw-event->riemann header) (:events response))
            ;; TODO: push the sort upstream
            events (sort-by :time events)
            many? (> (count events) 300)]
        (doseq [batch (partition-all 100 events)]
          (let [batch-results
                (->> batch
                     (map #(riemann/send-event rc %))
                     doall
                     (map #(deref % 2000 {:ok false})))]
            (when-not (every? :ok batch-results)
              (throw (Exception. "Error sending batch."))))
          ;; indicate progress
          (when many?
            (print ".")
            (flush)))
        (when many? (println))
        (println "Successfully sent" (count events) "events")))))

(defn -main [& args]
  (let [c (select-keys env [:api-url :riemann-host])]
    (when (seq c)
      (println "Using configuration" c)))
  (send-events
   (if (empty? args)
     identity
     (into #{} args))))
