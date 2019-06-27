(ns cfm-lastfm-import-acceptor.core
  (:require [clj-http.client :as client]
            [clojure.data.json :as json])
  (:import (com.amazonaws.services.sqs AmazonSQSClientBuilder))
  (:import (com.amazonaws.services.sqs AmazonSQS))
  (:import (com.amazonaws.services.sqs.model SendMessageBatchRequest SendMessageBatchRequestEntry))
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestHandler]))

(defn parse-int [number-string] (try (Integer/parseInt number-string) (catch Exception e nil)))

(defn get-track-chunk [user page]
  (client/get "https://ws.audioscrobbler.com/2.0/"
              {:throw-exceptions true
               :query-params {"method" "user.getrecenttracks"
                              "format" "json"
                              "user" user
                              "api_key" (System/getenv "LASTFM_KEY")
                              "page" page
                              "limit" 200}
               :as :json}))

(defn get-total-pages [user]
  (parse-int
   (get-in (json/read-str (:body (get-track-chunk user 1)))
           ["recenttracks", "@attr", "totalPages"])))

(defn -handleRequest [this request context]
  (let [user (get request "user")
        sqs (AmazonSQSClientBuilder/defaultClient)]
    (.sendMessageBatch sqs
                       (-> (SendMessageBatchRequest.)
                           (.withQueueUrl "https://sqs.eu-central-1.amazonaws.com/512170487137/import-tasks")
                           (.withEntries
                            (map #(SendMessageBatchRequestEntry. (str %)
                                                                 (json/write-str (hash-map "page" % "user" user)))
                                 (range 1 (inc (get-total-pages user)))))))))
