(ns congraph.core
  (:require [clojure.java.io :as io]
            [clojure.set :refer [rename-keys]]
            [hugcypher.core :as hugcypher]
            [hugsql.core :as hugsql]
            [hugcypher.cypher :as cypher]
            [hikari-cp.core :refer [make-datasource]]
            [hugsql.adapter.next-jdbc :as next-adapter]
            [to-jdbc-uri.core :refer [to-jdbc-uri]])
  (:import (clojure.lang IDeref)))

(defn validate-files [filenames]
  (doseq [file filenames]
    (when-not (or (instance? java.io.File file) (io/resource file))
      (throw (Exception. (str "congraph could not find the query file:" file))))))

(defn try-snip [[id snip]]
  [id
   (update snip :fn
           (fn [snip]
             (fn [& args]
               (try (apply snip args)
                    (catch Exception e
                      (throw (Exception. (str "Exception in " id) e)))))))])

(defn try-query [[id query]]
  [id
   (update query :fn
           (fn [query]
             (fn
               ([conn params]
                (try (query conn params)
                     (catch Exception e
                       (throw (Exception. (str "Exception in " id) e)))))
               ([conn params opts & command-opts]
                (try (apply query conn params opts command-opts)
                     (catch Exception e
                       (throw (Exception. (str "Exception in " id) e))))))))])


(defn load-queries [type & args]
  (let [options?  (map? (first args))
        options   (if options? (first args) {})
        filenames (if options? (rest args) args)]
    (validate-files filenames)
    (reduce
     (fn [queries file]
       (let [{snips true  fns   false} (group-by
                                        #(-> % second :meta :snip? boolean)
                                        (if (some #(= type %)
                                                  [:arcadedb-http :neo4j
                                                   :arcadedb-psql])
                                          (hugcypher/map-of-db-fns file options)
                                          (hugsql/map-of-db-fns file options)))]
         (-> queries
             (update :snips (fnil into {}) (mapv try-snip snips))
             (update :fns (fnil into {}) (mapv try-query fns)))))
     {}
     filenames)))

(defn intern-fn [ns id meta f]
  (intern ns (with-meta
               (symbol (name id))
               (-> meta
                   (assoc :arglists
                          (if (empty? (:keys (second (first (:arglists meta)))))
                            `([] [~'conn & ~'args])
                            `([~(second (first (:arglists meta)))]
                              [~'conn ~(second (first (:arglists meta))) & ~'args]))))) f))

(defmacro bind-connection [type conn & filenames]
  `(let [{snips# :snips fns# :fns :as queries#} (congraph.core/load-queries ~type ~@filenames)]
     (doseq [[id# {fn# :fn meta# :meta}] snips#]
       (congraph.core/intern-fn *ns* id# meta# fn#))
     (doseq [[id# {query# :fn meta# :meta}] fns#]
       (congraph.core/intern-fn *ns* id# meta#
                              (fn f#
                                ([] (if (some #(= ~type %) [:sql])
                                      (query# ~conn {})
                                      (query# ~type ~conn {})))
                                ([params#] (if (some #(= ~type %) [:sql])
                                             (query# ~conn params#)
                                             (query# ~type ~conn params#)))
                                ([conn# params# & args#] (if (some #(= ~type %) [:sql])
                                                           (apply query# conn# params# args#)
                                                           (apply query# ~type conn# params# args#))))))
     queries#))

(defmacro bind-connection-with-ns [ns type conn & filenames]
  `(let [{snips# :snips fns# :fns :as queries#} (congraph.core/load-queries ~type ~@filenames)]
     (doseq [[id# {fn# :fn meta# :meta}] snips#]
       (congraph.core/intern-fn ~ns id# meta# fn#))
     (doseq [[id# {query# :fn meta# :meta}] fns#]
       (congraph.core/intern-fn ~ns id# meta#
                              (fn f#
                                ([] (if (some #(= ~type %) [:sql])
                                      (query# ~conn {})
                                      (query# ~type ~conn {})))
                                ([params#] (if (some #(= ~type %) [:sql])
                                             (query# ~conn params#)
                                             (query# ~type ~conn params#)))
                                ([conn# params# & args#] (if (some #(= ~type %) [:sql])
                                                           (apply query# conn# params# args#)
                                                           (apply query# ~type conn# params# args#))))))
     queries#))

(defn- format-url [pool-spec]
  (if (:jdbc-url pool-spec)
    (update pool-spec :jdbc-url to-jdbc-uri)
    pool-spec))

(defn make-config [{:keys [jdbc-url adapter datasource datasource-classname] :as pool-spec}]
  (when (not (or jdbc-url adapter datasource datasource-classname))
    (throw (Exception. "one of :jdbc-url, :adapter, :datasource, or :datasource-classname is required to initialize the connection!")))
  (-> pool-spec
      (format-url)
      (rename-keys
        {:auto-commit?  :auto-commit
         :conn-timeout  :connection-timeout
         :min-idle      :minimum-idle
         :max-pool-size :maximum-pool-size})))

(defn connect [type config]
  (case type
    :neo4j (cypher/connect type {:url (:url config)
                                 :port (:port config)
                                 :username (:username config)
                                 :password (:password config)
                                 :opts (:opts config)})
    :arcadedb-http true
    :sql (do
           (hugsql/set-adapter! (next-adapter/hugsql-adapter-next-jdbc))
           (make-datasource (make-config config)))
    true nil))

(defn disconnect
  [type conn]
  (case type
    :neo4j (.close conn)
    :sql (when (and (instance? com.zaxxer.hikari.HikariDataSource conn)
                    (not (.isClosed conn)))
           (.close conn))))

(defn set-audit-defaults
  [type & {:keys [node-label node-id-attribute
                  default-node-id audit-relationship
                  audit-node audit-made-relationship]
           :as audit-params}]
  (cond
    (some #(= type %) [:arcadedb-http :neo4j
                       :arcadedb-psql])
    (cypher/set-audit-params audit-params)
    true nil))
