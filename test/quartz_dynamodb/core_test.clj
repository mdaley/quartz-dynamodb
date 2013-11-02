(ns quartz-dynamodb.core-test
  (:require [clojurewerkz.quartzite.jobs :as j]
            [midje.sweet :refer :all]
            [quartz-dynamodb.core :refer :all])
  (:import com.mdaley.quartz.dynamodb.DynamoDbJobStore
           org.quartz.simpl.SimpleClassLoadHelper
           org.quartz.SchedulerConfigException
           org.quartz.JobBuilder))

(def dynamo-endpoint "http://dynamodb.us-east-1.amazonaws.com")
(def quartz-prefix "Q_")
(def class-loader (SimpleClassLoadHelper.))
(def job-store (doto (DynamoDbJobStore.)
                 (.setDynamoDbDetails dynamo-endpoint quartz-prefix)
                 (.initialize class-loader nil)))

(j/defjob empty-job [ctx])

(defn make-empty-job
  [name group]
  (j/build
   (j/of-type empty-job)
   (j/with-identity name group)
   (j/with-description "empty job")))

(fact "Create dynamodb job store"
      (let [instance (DynamoDbJobStore.)]
        (.setDynamoDbDetails instance dynamo-endpoint quartz-prefix)
        (.initialize instance nil nil)
        (instance? DynamoDbJobStore instance) => true))

(fact "Creating dynamodb job store fails if details aren't set"
      (let [instance (DynamoDbJobStore.)]
        (.initialize instance nil nil) => (throws SchedulerConfigException)))

(fact "Create a new job"
      (let [job (make-empty-job "job1" "group1")]
        (.storeJob job-store job true)))
