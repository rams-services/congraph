(ns congraph.core
  (:require [clojure.java.io :as io]
            [clojure.set :refer [rename-keys]]
            [hugneo4j.core :as hugneo4j]
            [hugneo4j.cypher :as cypher])
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


(defn load-queries [& filenames]
  (validate-files filenames)
  (reduce
   (fn [queries file]
     (let [{snips true  fns   false} (group-by
                                      #(-> % second :meta :snip? boolean)
                                      (hugneo4j/map-of-db-fns file {}))]
       (-> queries
           (update :snips (fnil into {}) (mapv try-snip snips))
           (update :fns (fnil into {}) (mapv try-query fns)))))
   {}
   filenames))

(defn intern-fn [ns id meta f]
  (intern ns (with-meta
               (symbol (name id))
               (-> meta
                   (assoc :arglists
                          (if (empty? (:keys (second (first (:arglists meta)))))
                            `([] [~'conn & ~'args])
                            `([~(second (first (:arglists meta)))]
                              [~'conn ~(second (first (:arglists meta))) & ~'args]))))) f))

(defmacro bind-connection [conn & filenames]
  `(let [{snips# :snips fns# :fns :as queries#} (congraph.core/load-queries ~@filenames)]
     (doseq [[id# {fn# :fn meta# :meta}] snips#]
       (congraph.core/intern-fn *ns* id# meta# fn#))
     (doseq [[id# {query# :fn meta# :meta}] fns#]
       (congraph.core/intern-fn *ns* id# meta#
                              (fn f#
                                ([] (query# ~conn {}))
                                ([params#] (query# ~conn params#))
                                ([conn# params# & args#] (apply query# conn# params# args#)))))
     queries#))

(defn connect
  ([bolt-url] (connect bolt-url nil nil))
  ([bolt-url username] (connect bolt-url username nil))
  ([bolt-url username password]
   (cypher/connect bolt-url username password)))

(defn disconnect
  [conn]
  (.close conn))

(defn set-audit-defaults
  [& {:keys [node-label node-id-attribute
             default-node-id audit-relationship
             audit-node audit-made-relationship]
      :as audit-params}]
  (cypher/set-audit-params audit-params))
