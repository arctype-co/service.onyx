(ns arctype.service.onyx
  (:import
    [java.util UUID])
  (:require
    [clojure.core.async :as async :refer [go <!]]
    [clojure.tools.logging :as log]
    [arctype.service.protocol :refer :all]
    [arctype.service.util :refer [<?? thread-try]]
    [arctype.service.curator :as curator]
    [arctype.service.quartzite :as quartzite]
    [arctype.service.onyx.engine :as onyx-engine]
    [arctype.service.onyx.janitor :as janitor]
    [cheshire.core :as json]
    [compojure.core :as compojure-core]
    [compojure.api.sweet :as compojure-api :refer [context GET POST ANY]]
    [muuntaja.core :as muuntaja]
    [onyx.api :as onyx-api]
    [onyx.extensions :as extensions]
    [onyx.log.zookeeper :as zk-log]
    [schema.core :as S]
    [sundbry.resource :as resource :refer [with-resources]]
    [arctype.service.onyx.schema :refer :all]))

(def Config
  {:onyx onyx-engine/Config
   :curator curator/Config
   :jobs {S/Keyword S/Any}
   ; Swagger API info
   :info {:version S/Str :title S/Str :description S/Str}
   (S/optional-key :janitor) janitor/Config})

(def ^:private default-config
  {:janitor {}})

(defn- maybe-int
  [x]
  (when (integer? x) x))

(defn handle-internal-exception
  [^Exception ex data request]
  (let [data (condp = (class ex)
               org.httpkit.client.TimeoutException (assoc data :http-status 504)
               data)
        response {:status (or (:http-status data) (maybe-int (:status data)) 500)
                  :headers (merge {"Access-Control-Allow-Origin" "*"} (:http-headers data))
                  :body (or (:http-body data) {:message (str "Service error: " (.getMessage ex))})}]
    (when (= 500 (:status response))
      (log/error ex {:message (str "Internal server error:" (.getMessage ex))
                     :data data}))
    response))

(defn handle-validation-exception
  [^Exception ex data request]
  (log/error {:message "Request schema error"
              :data (select-keys data [:schema :value :errors])})
  (let [error-type  (case (:type data)
                      :compojure.api.exception/request-validation "request "
                      :compojure.api.exception/response-validation "response "
                      "")]
    {:status 400
     :headers {"Access-Control-Allow-Origin" "*"}
     :body {:message (str "Server " error-type "schema error: " (print-str (:error data)))}}))

(defn- response-str
  [response]
  (update response :body str))

(defn- sanitize-request
  [req]
  (-> req 
      (update :headers
              (fn [headers]
                (cond-> headers
                  (some? (get headers "authorization"))
                  (assoc "authorization" "<redacted>"))))))

(defn wrap-tracing
  [handler]
  (fn [req]
    (let [request-id (str (UUID/randomUUID))
          nano-time (System/nanoTime)
          req (assoc req :request-id request-id)]
      (log/trace {:message "API request"
                  :request-id request-id
                  :request (sanitize-request req)})
      (let [response (handler req)
            dt (- (System/nanoTime) nano-time)]
        (log/trace {:message "API response"
                    :request-id request-id
                    :elapsed (/ (double dt) 1e9)
                    :response (response-str response)})
        response))))

(defn- check-error
  [result]
  (when-let [ex (:e result)]
    (throw ex))
  (when-not (:success? result)
    (throw (ex-info "Onyx job submittal unknown error, check the logs." result)))
  result)

(S/defn ^:private submit-job! :- OnyxJob
  [{:keys [state] :as this} job-builder job-config]
  (with-resources this [:onyx]
    (let [onyx-client (client onyx)
          job-spec (job-builder (:job-context @state) job-config)
          result (onyx-api/submit-job onyx-client job-spec)]
      (check-error result)
      (log/info {:message "Onyx job submitted"
                 :spec job-spec
                 :result result})
      {:job-id (str (:job-id result))
       :job-name (:job-name job-spec)})))

(S/defn ^:private resume-job! :- OnyxResumedJob
  [{:keys [state] :as this}
   {:keys [job-id job-name] :as params} :- ResumeJobParams
   job-builder
   job-config]
  (with-resources this [:onyx]
    (let [onyx-client (client onyx)
          tenancy-id (onyx-engine/tenancy-id onyx)
          new-job (job-builder (:job-context @state) job-config)
          snapshot (if (some? job-id)
                     (onyx-api/job-snapshot-coordinates onyx-client tenancy-id job-id)
                     (onyx-api/named-job-snapshot-coordinates onyx-client tenancy-id job-name))
          _ (when (nil? snapshot)
              (throw (ex-info "Job snapshot not found" params)))
          resume-point (onyx-api/build-resume-point new-job snapshot)
          new-job (assoc new-job :resume-point resume-point)
          result (onyx-api/submit-job onyx-client new-job)]
      (check-error result)
      (log/info {:message "Onyx job resumed"
                 :snapshot snapshot
                 :resume-point resume-point
                 :result result})
      {:snapshot snapshot
       :job-id (str (:job-id result))
       :job-name (:job-name new-job)})))

(S/defn ^:private kill-job!
  [this
   {:keys [job-id job-name]} :- JobKillRequest]
  (with-resources this [:onyx]
    (let [job-id (or job-id (:job-id (last (onyx-api/job-ids-history (client onyx) (onyx-engine/tenancy-id onyx) job-name))))
          result (onyx-engine/kill-job onyx job-id)]
      
      (if result
        (do 
          (log/info {:message "Job killed"
                       :job-name job-name
                       :job-id job-id
                       :result result})
          {:job-id job-id})
        (throw (ex-info "Failed to kill job" {:result result :job-name job-name :job-id job-id}))))))

(S/defn ^:private job-state
  [{:keys [state] :as this} {:keys [job-id] :as params}]
  (with-resources this [:onyx]
    (let [onyx-client (client onyx)
          tenancy-id (onyx-engine/tenancy-id onyx)
          current-replica {:replica (:replica @state)
                           :alive? (zk-log/tenancy-alive? (:conn (:log onyx-client)) tenancy-id)}]
      (onyx-api/job-state current-replica tenancy-id (UUID/fromString job-id)))))

(defn- submit-job-name!
  [{:keys [config job-defs] :as this} {:keys [job-name] :as params}]
  (if-let [job-def (get job-defs job-name)]
    (let [job-config (get-in config [:jobs (keyword job-name)])]
      (submit-job! this job-def job-config))
    (throw (ex-info "Undefined job name" params))))

(defn- resume-job-name!
  [{:keys [config job-defs] :as this} 
   {:keys [job-name] :as params}]
  (if-let [job-def (get job-defs job-name)]
    (let [job-config (get-in config [:jobs (keyword job-name)])]
      (resume-job! this params job-def job-config))
    (throw (ex-info "Undefined job name" params))))

(defn- job-ids
  [{:keys [state]}]
  (->> (select-keys (:replica @state) [:jobs :killed-jobs])
       (vals)
       (apply concat)))

(defn- job-index
  [{:keys [state] :as this}]
  (with-resources this [:onyx]
    (let [onyx-client (client onyx)
          tenancy-id (onyx-engine/tenancy-id onyx)
          current-replica {:replica (:replica @state)
                           :alive? (zk-log/tenancy-alive? (:conn (:log onyx-client)) tenancy-id)}
          jobs (->> (for [job-id (job-ids this)]
                      [job-id (assoc (onyx-api/job-state current-replica tenancy-id job-id)
                                     :job-id job-id)])
                    (into {}))]
      jobs)))

(defn- onyx-status
  [{:keys [config state] :as this}]
  (with-resources this [:onyx]
    (let [onyx-client (client onyx)
          tenancy-id (onyx-engine/tenancy-id onyx)
          current-replica {:replica (:replica @state)
                           :alive? (zk-log/tenancy-alive? (:conn (:log onyx-client)) tenancy-id)}
          jobs (->> (for [[job-name job-cfg] (:jobs config)]
                      (let [{:keys [job-id]} (last (onyx-api/job-ids-history onyx-client tenancy-id (name job-name)))
                            j-state (onyx-api/job-state current-replica tenancy-id job-id)]
                        [job-name (assoc j-state
                                         :job-id job-id)]))
                    (into {}))]
      {:jobs jobs})))

(defn- healthy-onyx-status?
  [{:keys [jobs]}]
  (->> (vals jobs)
       (map (fn [{:keys [job-state]}]
              (case job-state
                :running true
                false)))
       (reduce #(and %1 %2) true)))

(defn- http-health-check
  [this req]
  (let [result (onyx-status this)
        healthy? (healthy-onyx-status? result)]
    (if healthy?
      (log/info {:message "Health check OK"
                 :status result})
      (log/warn {:message "Health check FAILED"
                 :status result}))
    {:status (if healthy? 200 500)
     :body result}))

(defn- deep-gc!
  [this]
  (with-resources this [:onyx]
    (if (healthy-onyx-status? (onyx-status this))
      (let [onyx-client (client onyx)
            tenancy-id (onyx-engine/tenancy-id onyx)]
        (log/info {:message "Performing checkpoint garbage collection"})
        (doseq [job-id (job-ids this)]
          (log/info (merge
                      {:message "Garbage collected checkpoints for job"
                       :tenancy-id tenancy-id
                       :job-id job-id}
                      (onyx-api/gc-checkpoints onyx-client tenancy-id job-id))))
        (if (healthy-onyx-status? (onyx-status this))
          (do
            (log/info {:message "Performing log garbage collection"})
            (onyx-api/gc onyx-client)
            (log/info {:message "Garbage collection complete"}))
          (log/warn {:message "Log garbage collection blocked"})))
      (log/warn {:message "Garbage collection blocked"}))))

(defn- handle-log-entry
  [{:keys [state]} entry]
  (swap! state update :replica
         #(extensions/apply-log-entry entry (assoc % :version (:message-id entry)))))

(defn- handle-events
  [{:keys [events-chan] :as this}]
  (go
    (try
      (loop []
        (when-let [event (<! events-chan)]
          (log/debug {:message "Handle onyx event"
                      :event event})
          (handle-log-entry this event)
          (recur)))
      (catch Throwable e
        (log/fatal e {:message "Onyx event handler crashed!"})))
    (log/debug {:message "Event handler stopped"})))

(defn- initial-state
  []
  {:job-context nil
   :replica nil})

(defn set-job-context!
  "Set the context for job construction"
  [{:keys [state]} context]
  (swap! state assoc :job-context context))

(defrecord DataService [config state job-defs]

  PLifecycle
  (start [this]
    (with-resources this [:onyx :janitor]
      (log/info {:message "Starting data service"
                 :job-defs (keys job-defs)})
      (reset! state (initial-state))
      (janitor/set-cleaner! janitor #(deep-gc! this))
      (as-> this this
        (assoc this :events-chan (async/chan))
        (assoc this :sub (onyx-api/subscribe-to-log (client onyx) (:events-chan this)))
        (do
          (swap! state assoc :replica (:replica (:sub this)))
          this)
        (assoc this :events-handler (handle-events this)))))

  (stop [this]
    (log/info {:message "Stopping data service"})
    (async/close! (:events-chan this))
    (<?? (:events-handler this))
    (dissoc this :events-chan :sub :events-handler))

  PHttpHandler
  (ring-handler [this]
    (-> (compojure-api/api
          {:formats muuntaja/default-options
           :exceptions {:handlers {:compojure.api.exception/default handle-internal-exception
                                   :compojure.api.exception/request-validation handle-validation-exception
                                   :compojure.api.exception/response-validation handle-validation-exception}}
           :swagger {:spec "/swagger.json"
                     :ui "/swagger"
                     :data {:info (:info config)}}}
          (context "/" []
                   :dynamic true
                   (GET "/" []
                        {:status 200
                         :body (:title (:info config))})
                   (GET "/health" req
                        :summary "Perform a health check"
                        (http-health-check this req))
                   (context "/job" []
                            (GET "/" req
                                 :return S/Any ;JobIndex
                                 :summary "Get job list"
                                 {:status 200
                                  :body (job-index this)})
                            (POST "/kill" req
                                  :body [params JobKillRequest]
                                  :return S/Any
                                  :summary "Kill a job"
                                  {:status 200
                                   :body (kill-job! this params)})
                            (GET "/id/:job-id" req
                                 :return JobState
                                 :summary "Get job state"
                                 {:status 200
                                  :body (job-state this (:params req))})
                            (context "/:job-name" []
                                     (POST "/start" req
                                           :return OnyxJob
                                           :summary "Submit a named job"
                                           {:status 202
                                            :body (submit-job-name! this (:params req))})
                                     (POST "/resume" req
                                           :body [params (S/maybe JobSelect)]
                                           :return OnyxResumedJob
                                           :summary "Resume a named job"
                                           {:status 202
                                            :body (resume-job-name! this (:params req))})
                                     (POST "/kill" req
                                           :return S/Any
                                           :summary "Kill a named job"
                                           {:status 200
                                            :body (kill-job! this (:params req))})))
                   (POST "/gc" req
                         :summary "Garbage collect"
                         (with-resources this [:janitor]
                           (janitor/request-gc! janitor)
                           {:status 201
                            :body nil}))))
        wrap-tracing))

)

(defn- index-job-defs
  [job-defs-seq]
  (->> (seq job-defs-seq)
       (map (fn [job-def]
              (let [job-meta (meta job-def)
                    job-name (or (:job-name job-meta) 
                                 (throw (ex-info "Undefined job name" (or job-meta {}))))]
                [job-name @job-def])))
       (into {})))

(S/defn create
  [resource-name
   config :- Config
   job-def-set]
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->DataService
        {:config config
         :state (atom {})
         :job-defs (index-job-defs job-def-set)})
      resource-name
      #{}
      [(curator/create :curator (:curator config))
       (onyx-engine/create :onyx (:onyx config))
       (janitor/create :janitor (:janitor config))
       (quartzite/create :quartzite {})])))
