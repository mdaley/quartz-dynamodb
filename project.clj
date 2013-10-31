(defproject quartz-dynamodb "0.0.1-SNAPSHOT"
  :description "Allow quartz and quartzite to be backed by AWS DynamoDB."
  :url "https://github.com/mdaley/quartz-dynamodb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.amazonaws/aws-java-sdk "1.6.2"]
                 [joda-time/joda-time "2.3"]
                 ;;[clj-time "0.6.0"]
                 [midje "1.5.1"]
                 [org.quartz-scheduler/quartz "2.2.1"]]
  :java-source-paths ["src/java"]
  :javac-options     ["-target" "1.6" "-source" "1.6"]
  :profiles {:dev {:plugins [[lein-rpm "0.0.5"]
                             [lein-midje "3.1.1"]]}})
