(defproject cfm-lastfm-import-acceptor "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [clj-http "3.9.1"]
                 [org.clojure/data.json "0.2.6"]
                 [com.amazonaws/aws-java-sdk-sqs "1.11.580"]
                 [com.amazonaws/aws-lambda-java-core "1.2.0"]]
  :main cfm-lastfm-import-acceptor.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
