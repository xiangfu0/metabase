(ns metabase.driver.pinot
  "Pinot driver."
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [metabase.driver :as driver]
   [metabase.driver.pinot.client :as pinot.client]
   [metabase.driver.pinot.execute :as pinot.execute]
   [metabase.driver.pinot.query-processor :as pinot.qp]
   [metabase.driver.pinot.sync :as pinot.sync]
   [metabase.query-processor.pipeline :as qp.pipeline]
   [metabase.util.log :as log]
   [metabase.util.ssh :as ssh]))

(driver/register! :pinot)

(doseq [[feature supported?] {:expression-aggregations        true
                              :schemas                        false
                              :set-timezone                   true
                              :temporal/requires-default-unit true}]
  (defmethod driver/database-supports? [:pinot feature] [_driver _feature _db] supported?))

(defmethod driver/can-connect? :pinot
  [_ details]
  {:pre [(map? details)]}
  (ssh/with-ssh-tunnel [details-with-tunnel details]
    (let [{:keys [auth-enabled auth-username auth-token-value]} details]
      (= 200 (:status (http/get (pinot.client/details->url details-with-tunnel "/health")
                                (cond-> {}
                                  auth-enabled (assoc :basic-auth (str auth-username ":" auth-token-value)))))))))

(defmethod driver/describe-table :pinot
  [_ database table]
  (pinot.sync/describe-table database table))

(defmethod driver/dbms-version :pinot
  [_ database]
  (pinot.sync/dbms-version database))

(defmethod driver/describe-database :pinot
  [_ database]
  (pinot.sync/describe-database database))

(defmethod driver/mbql->native :pinot
  [_ query]
  (pinot.qp/mbql->native query))

(defn- add-timeout-to-query [query timeout]
  (let [parsed (if (string? query)
                 (json/parse-string query keyword)
                 query)]
    (assoc-in parsed [:context :timeout] timeout)))

(defmethod driver/execute-reducible-query :pinot
  [_driver query _context respond]
   (log/debugf "Executing reducible Pinot query: %s" query)
  (pinot.execute/execute-reducible-query
   (partial pinot.client/do-query-with-cancellation qp.pipeline/*canceled-chan*)
   (update-in query [:native :query] add-timeout-to-query qp.pipeline/*query-timeout-ms*)
   respond))

(defmethod driver/db-start-of-week :pinot
  [_]
  :sunday)
