(ns ^{:doc "Steelyx service schemas"}
  steelyx.schema
  (:require
    [schema.core :as S]))

(def JobSelect
  {(S/optional-key :job-id) S/Str
   (S/optional-key :job-name) S/Str})

(def JobKillRequest
  JobSelect)

(def JobState
  S/Any)

(def OnyxJob
  {:job-id S/Str
   :job-name (S/maybe S/Str)})

(def OnyxResumedJob
  (assoc OnyxJob
         :snapshot {S/Keyword S/Any}))

(def ResumeJobParams
  JobSelect)

(def GcOptions
  {(S/optional-key :force?) S/Bool})
