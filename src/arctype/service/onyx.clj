(ns arctype.service.onyx
  (:import 
    [java.util UUID])
  (:require
    [clojure.string :as string]
    [clojure.tools.logging :as log]
    [arctype.service.protocol :refer :all]
    [arctype.service.util :refer [rmerge]]
    [com.palletops.log-config.timbre.tools-logging :refer [make-tools-logging-appender]]
    [com.stuartsierra.component :as component]
    [onyx.api :as onyx]
    [onyx.schema :as onyx-schema]
    [onyx.system :as onyx-system]
    [schema.core :as S]
    [schema.coerce :as coerce]
    [sundbry.resource :as resource]))

(def Config
  ; Full peer config schema at https://github.com/onyx-platform/onyx/blob/0.9.x/src/onyx/schema.cljc#L530
  {:peer-config {:zookeeper/address S/Str ; "127.0.0.1:2188"
                 :onyx/tenancy-id S/Str
                 S/Keyword S/Any}
   :n-peers S/Int ; Number of peers to spawn within this instance
   })

(def ^:private default-config
  {:peer-config {:onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                 :onyx.messaging/impl :aeron
                 :onyx.messaging/peer-port 40200
                 :onyx.messaging/bind-addr "localhost"}
   :n-peers 2})

(defn- env-config
  []
  (let [env-onyx-bind-addr (System/getenv "ONYX_BIND_ADDR")]
    (if (and (some? env-onyx-bind-addr)
             (not (empty? env-onyx-bind-addr)))
      {:peer-config {:onyx.messaging/bind-addr env-onyx-bind-addr}}
      {})))

(defn string->namespaced-keyword [s]
  (if (string? s) 
    (let [[s-ns s-kw] (string/split s #"/")]
      (if (some? s-kw)
        (keyword s-ns s-kw)
        (keyword s-ns)))
    s))

(defn- namespaced-json-coercion-matcher
  [schema]
  (or 
    ({onyx-schema/NamespacedKeyword string->namespaced-keyword} schema)
    (coerce/+json-coercions+ schema)
    (coerce/keyword-enum-matcher schema)
    (coerce/set-matcher schema)))

(S/defn zookeeper-address :- S/Str
  [this]
  (-> this :config :peer-config :zookeeper/address))

(S/defn tenancy-id :- S/Str
  [this]
  (:onyx/tenancy-id (:peer-config this)))

(defn submit-job
  [this job]
  (onyx/submit-job (:client this) job))

(defn kill-job
  [this job-id]
  (onyx/kill-job (:client this) job-id))

(defn job-state
  [this job-id]
  (onyx/job-state (:client this) (tenancy-id this) job-id))

(defn gc
  [this]
  (onyx/gc (:client this)))

(defrecord OnyxService [config]
  PLifecycle

  (start [this]
    (log/info {:message "Starting Onyx service"
               :config config
               :tenancy-id (:onyx/tenancy-id (:peer-config config))})
    (let [peer-config (assoc (:peer-config config)
                             :onyx.log/config 
                             {:appenders
                              {:println nil
                               :jl (make-tools-logging-appender
                                     {:enabled? true
                                      :fmt-output-opts {:nofonts? true}})}
                              :min-level :info})
          coerce-peer-config (coerce/coercer onyx-schema/PeerConfig namespaced-json-coercion-matcher)
          peer-config (coerce-peer-config peer-config)
          _ (when (schema.utils/error? peer-config)
              (throw (ex-info (str (schema.utils/error-val peer-config))
                              peer-config)))
          peer-group (onyx/start-peer-group peer-config)
          peers (onyx/start-peers (:n-peers config) peer-group)
          client (component/start (onyx-system/onyx-client peer-config))]
      (assoc this
             :peer-config peer-config
             :peer-group peer-group
             :peers peers
             :client client)))

  (stop [this]
    (log/info {:message "Stopping Onyx service"})
    (doseq [peer (:peers this)]
      (onyx/shutdown-peer peer))
    (onyx/shutdown-peer-group (:peer-group this))
    (as-> this this
      (update this :client component/stop)
      (dissoc this :tenancy-id :peer-group :peers :client)))

  PClientDecorator
  (client [this] (:client this))
  
  )

(S/defn create
  [resource-name
   config :- Config]
  (let [config (rmerge (rmerge default-config (env-config)) config)]
    (resource/make-resource
      (map->OnyxService
        {:config config})
      resource-name)))
