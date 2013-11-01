(ns quartz-dynamodb.core-test
  (:require ;[environ.core :refer [env]]
            [midje.sweet :refer :all]
            [quartz-dynamodb.core :refer :all])
  (:import com.mdaley.quartz.dynamodb.DynamoDbJobStore
           org.quartz.SchedulerConfigException))

(def dynamo-endpoint "http://dynamodb.us-east-1.amazonaws.com")
(def quartz-prefix "Q_")

(fact "I am a simple test that works"
      (= 1 1) => true)

(fact "Create dynamodb job store"
      (let [instance (DynamoDbJobStore.)]
        (.setDynamoDbDetails instance dynamo-endpoint quartz-prefix)
        (.initialize instance nil nil)
        (instance? DynamoDbJobStore instance) => true))

(fact "Creating dynamodb job store fails if details aren't set"
      (let [instance (DynamoDbJobStore.)]
        (.initialize instance nil nil) => (throws SchedulerConfigException)))
