(ns metabase.driver.pinot.client
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [clojure.core.async :as a]
   [metabase.models.secret :as secret]
   [metabase.query-processor.error-type :as qp.error-type]
   [metabase.util :as u]
   [metabase.util.i18n :refer [tru]]
   [metabase.util.log :as log]
   [metabase.util.ssh :as ssh]))

(set! *warn-on-reflection* true)

(defn details->url
  "Helper for building a Pinot URL.
   (details->url {:controller-endpoint \"http://localhost:9000\"}) -> \"http://localhost:9000\""
  [{:keys [controller-endpoint]} & strs]
  {:pre [(string? controller-endpoint) (seq controller-endpoint)]}
  (apply str (format "%s" controller-endpoint) (map name strs)))

(defn- do-request
  "Perform a JSON request using `request-fn` against `url`.

     (do-request http/get \"http://my-json-api.net\")"
  [request-fn url & {:as options}]
  {:pre [(fn? request-fn) (string? url)]}
  ;; this is the way the `Content-Type` header is formatted in requests made by the Pinot web interface
  (let [{:keys [auth-enabled auth-token-type auth-token-value]} options
        ;; Construct headers map
        headers (cond-> {"Content-Type" "application/json;charset=UTF-8"}
                  auth-enabled (assoc "Authorization" (str auth-token-type " " auth-token-value)))

        ;; Update options with headers and possibly serialize body if present
        options (cond-> (merge options {:headers headers})
                  (:body options) (update :body json/generate-string))]
    (try
      (let [{:keys [status body]} (request-fn url options)]
        (log/debugf "Pinot request url: %s, body: %s" url body)
        (when (not= status 200)
          (throw (ex-info (tru "Pinot request error [{0}]: {1}" status (pr-str body))
                          {:type qp.error-type/db})))
        (try
          (json/parse-string body keyword)
          (catch Throwable e
            (throw (ex-info (tru "Failed to parse Pinot response body: {0}" (pr-str body))
                            {:type qp.error-type/db}
                            e)))))
      (catch Throwable e
        (let [response (u/ignore-exceptions
                         (when-let [body (:body (ex-data e))]
                           (json/parse-string body keyword)))]
          (throw (ex-info (or (:errorMessage response)
                              (.getMessage e))
                          (merge
                           {:type            qp.error-type/db
                            :request-url     url
                            :request-options options}
                           (when response
                             {:response response}))
                          e)))))))

(def ^{:arglists '([url & {:as options}]), :style/indent [:form]} GET    "Execute a GET request."    (partial do-request http/get))
(def ^{:arglists '([url & {:as options}]), :style/indent [:form]} POST   "Execute a POST request."   (partial do-request http/post))
(def ^{:arglists '([url & {:as options}]), :style/indent [:form]} DELETE "Execute a DELETE request." (partial do-request http/delete))

(defn do-query
  "Run a Pinot `query` against database connection `details`."
  [details query]
  {:pre [(map? details) (map? query)]}
  (ssh/with-ssh-tunnel [details-with-tunnel details]
    (try
      ;; Use the POST helper function to send the Pinot query
      (let [url (details->url details-with-tunnel "/sql")
            response (POST url
                       :body query
                       :auth-enabled     (:auth-enabled details)
                       :auth-token-type  (:auth-token-type details)
                       :auth-token-value (-> details
                                             (secret/db-details-prop->secret-map "auth-token-value")
                                             secret/value->string))]

        ;; Logging query, details, and parsed response
        (log/debugf "Pinot details: %s, Pinot query: %s, Parsed Pinot response: %s" details query response)

        ;; Return parsed response for post-processing
        response)

      ;; Handle interrupted queries
      (catch InterruptedException e
        (log/error e "Query was interrupted")
        (throw e))

      ;; Handle other exceptions
      (catch Throwable e
        (let [e' (ex-info (.getMessage e)
                          {:type  qp.error-type/db
                           :query query}
                          e)]
          (log/error e' "Error running query")
          ;; Re-throw a new exception with `message` set to the extracted message
          (throw e'))))))

(defn- cancel-query-with-id! [details query-id]
  (if-not query-id
    (log/warn "Client closed connection, no queryId found, can't cancel query")
    (ssh/with-ssh-tunnel [details-with-tunnel details]
      (log/warnf "Client closed connection, canceling Pinot queryId %s" query-id)
      (try
        (log/debugf "Canceling Pinot query with ID %s" query-id)
        (DELETE (details->url details-with-tunnel (format "/pinot/v2/%s" query-id))
          :auth-enabled  (:auth-enabled details)
          :auth-token-type  (:auth-token-type details)
          :auth-token-value (-> details
                                (secret/db-details-prop->secret-map "auth-token-value")
                                secret/value->string))
        (catch Exception cancel-e
          (log/warnf cancel-e "Failed to cancel Pinot query with queryId %s" query-id))))))

(defn do-query-with-cancellation
  "Run a Pinot `query`, canceling it if `canceled-chan` gets a message."
  [canceled-chan details query]
  {:pre [(map? details) (map? query)]}
  (let [query-id  (get-in query [:context :queryId])
        query-fut (future
                    (try
                      (do-query details query)
                      (catch Throwable e
                        e)))
        cancel! (delay
                  (cancel-query-with-id! details query-id))]
    (a/go
      (when (a/<! canceled-chan)
        (future-cancel query-fut)
        @cancel!))
    (try
      ;; Run the query in a future so that this thread will be interrupted, not the thread running the query (which is
      ;; not interrupt aware)
      (u/prog1 @query-fut
        (when (instance? Throwable <>)
          (throw <>)))
      (catch InterruptedException e
        @cancel!
        (throw e)))))
