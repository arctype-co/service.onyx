(ns steelyx.janitor
  (:require
    [clojure.tools.logging :as log]
    [arctype.service.protocol :refer :all]
    [arctype.service.fsm :as fsm]
    [arctype.service.curator :refer [curator-path]]
    [arctype.service.quartzite :as quartzite]
    [clojurewerkz.quartzite.jobs :as jobs]
    [clojurewerkz.quartzite.triggers :as triggers]
    [clojurewerkz.quartzite.scheduler :as scheduler]
    [curator.mutex :as curator-mutex]
    [schema.core :as S]
    [sundbry.resource :as resource :refer [with-resources]]))

(def Config
  {(S/optional-key :gc-times-24h) [S/Str] ; Times to execute garbage collection
   (S/optional-key :gc-time-zone) S/Str ; Time zone string (e.g. "America/Los_Angeles")
   (S/optional-key :stop-timeout-ms) S/Int
   (S/optional-key :mutex-timeout-ms) S/Int})

(def ^:private default-config
  {:gc-times-24h ["06:00"]
   :gc-time-zone "America/Los_Angeles"
   :stop-timeout-ms 30000
   :mutex-timeout-ms 10000})

(def ^:private global-instance (atom nil))

(defn request-gc!
  [{:keys [fsm]} options]
  (log/debug {:message "Requesting garbage collection"})
  (fsm/transition! fsm :request-gc options)
  nil)

(defn set-cleaner!
  "Provide a function to perform the garbage collection."
  [{:keys [state]} cleaner-fn]
  (swap! state assoc :cleaner cleaner-fn))

(comment
(defn- fsm-init
  [fsm {:keys [state] :as this}]
  (with-resources this [:curator]
    (log/debug {:message "Starting Janitor leader election"})
    (let [election (doto (curator-leader/leader-latch (client curator)
                                                      (curator-path curator "/janitor")
                                                      #(fsm/transition! fsm :lead)
                                                      :notleaderfn #(fsm/transition! fsm :follow)
                                                      :close-mode :notify-leader)
                     (.start))]
      (swap! state assoc :election election))))

(defn- fsm-following
  [fsm this]
  (log/debug {:message "Janitor is following"}))

(defn- fsm-leading
  [fsm this]
  (log/debug {:message "Janitor is leading"}))

(defn- fsm-shutdown
  [fsm {:keys [state] :as this}]
  (swap! state update :election (fn [election]
                                  (when (some? election)
                                    (log/debug {:message "Stopping Janitor leader election"})
                                    (.close election)
                                    nil))))

(def ^:private fsm-spec
  {:initial-state :init
   :terminal-states #{:shutdown}

   :states 
   {:init fsm-init
    :following fsm-following
    :leading fsm-leading
    :shutdown fsm-shutdown}

   :transitions 
   [[:init :follow :following]
    [:init :lead :leading]
    [:init :shutdown :shutdown]
    [:following :lead :leading]
    [:following :shutdown :shutdown]
    [:leading :follow :following]
    [:leading :shutdown :shutdown]]}))

(defn- fsm-idle
  [fsm {:keys [state]}]
  (when-not (:run? @state)
    (fsm/transition! fsm :shutdown)))

(defn- fsm-locking-gc
  ([fsm this]
   (fsm-locking-gc fsm this nil))
  ([fsm {:keys [config mutex]} {:keys [force?] :as options}]
   (try
     (log/debug {:message "Acquiring GC mutex"
                 :force? force?})
     (if (or force? (curator-mutex/acquire mutex (:mutex-timeout-ms config)))
       (fsm/transition! fsm :acquired options)
       (fsm/transition! fsm :timeout))
     (catch Exception e
       (log/error e {:message "Failure in mutex acquisition"})
       (fsm/transition! fsm :timeout)))))

(defn- fsm-performing-gc
  [fsm {:keys [state] :as this} options]
  (try 
    (log/info {:message "Checking status for garbage collection"})
    (let [{:keys [cleaner]} @state]
      (cleaner options))
    (catch Exception e
      (log/error e {:message "Failed garbage collection"}))
    (finally
      (fsm/transition! fsm :complete))))

(defn- fsm-releasing-gc
  [fsm {:keys [mutex]}]
  (try
    (log/debug {:message "Releasing GC mutex"})
    (curator-mutex/release mutex)
    (fsm/transition! fsm :released)
    (catch java.lang.IllegalStateException e
      (log/error e {:message "Failure in mutex release, not acquired."})
      (fsm/transition! fsm :released))
    (catch Exception e
      (log/error e {:message "Failure in mutex release, retrying."})
      (Thread/sleep 1000)
      (fsm/transition! fsm :retry))))

(def ^:private fsm-spec
  {:initial-state :idle
   :terminal-states #{:shutdown}

   :states 
   {:idle fsm-idle
    :locking-gc fsm-locking-gc
    :performing-gc fsm-performing-gc
    :releasing-gc fsm-releasing-gc
    :shutdown fsm/no-op}

   :transitions 
   [[:idle :request-gc :locking-gc]
    [:idle :shutdown :shutdown]
    [:locking-gc :acquired :performing-gc]
    [:locking-gc :timeout :idle]
    [:performing-gc :complete :releasing-gc]
    [:releasing-gc :released :idle]
    [:releasing-gc :retry :releasing-gc]]})

(defn- shutdown-fsm!
  [{:keys [fsm state] :as this}]
  (swap! state assoc :run? false)
  (fsm/transition! fsm :shutdown)
  (stop fsm)
  (dissoc this :fsm))

(defn- start-fsm!
  [{:keys [state config] :as this}]
  (swap! state assoc :run? true)
  (let [fsm (-> (fsm/create {:client this :spec fsm-spec :stop-timeout-ms (:stop-timeout-ms config)})
                (start))]
    (fsm/transition! fsm :start)
    (assoc this :fsm fsm)))

(jobs/defjob GcRequestJob
  [ctx]
  (let [instance @global-instance]
    (request-gc! instance)))

(defn- start-schedule!
  [{config :config
    :as this}]
  (with-resources this [:quartzite]
    (let [qz (client quartzite)
          gc-request-job-key (jobs/key (str "gc-request-job-" (hash this)) (resource/full-name this))
          job (jobs/build
                (jobs/of-type GcRequestJob)
                (jobs/with-identity gc-request-job-key)
                (jobs/store-durably))]
      (scheduler/add-job qz job)
      (doseq [schedule (quartzite/make-daily-schedules (:gc-times-24h config) (:gc-time-zone config))
              :let [trigger-key (triggers/key (str "gc-request-trigger-" (hash schedule)) (resource/full-name this))
                    trigger (triggers/build
                              (triggers/with-identity trigger-key)
                              (triggers/with-schedule schedule)
                              (triggers/for-job gc-request-job-key))]]
        (scheduler/add-trigger qz trigger))
      (assoc this :gc-request-job-key gc-request-job-key))))

(defn- stop-schedule!
  [{:keys [gc-request-job-key] :as this}]
  (if (some? gc-request-job-key)
    (with-resources this [:quartzite]
      (scheduler/delete-job (client quartzite) gc-request-job-key)
      (dissoc this :gc-request-job-key))
    this))

(defrecord Janitor [config]
  PLifecycle
  
  (start [this]
    (log/debug {:message "Starting Janitor"})
    (with-resources this [:curator]
      (as-> this this
        (assoc this :mutex (curator-mutex/semaphore-mutex 
                             (client curator)
                             (curator-path curator "/janitor/gc-mutex")))
        (start-fsm! this)
        (start-schedule! this)
        (do
          (reset! global-instance this)
          this))))
  
  (stop [this]
    (log/debug {:message "Stopping Janitor"})
    (as-> this this
      (stop-schedule! this)
      (shutdown-fsm! this)
      (dissoc this :mutex))))

(S/defn create
  [resource-name
   config :- Config]
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->Janitor {:config config
                     :state (atom {:cleaner (constantly false)})})
      resource-name
      #{:curator :quartzite})))
