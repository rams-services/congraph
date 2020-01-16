(ns congraph.core
  (:require [clojure.java.io :as io]
            [clojure.set :refer [rename-keys]]
            [hugneo4j.core :as hugneo4j])
  (:import (clojure.lang IDeref)))

(defn validate-files [filenames]
  (doseq [file filenames]
    (when-not (or (instance? java.io.File file) (io/resource file))
      (throw (Exception. (str "conman could not find the query file:" file))))))

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
  (intern ns (with-meta (symbol (name id)) meta) f))

(defmacro bind-connection [conn & filenames]
  `(let [{snips# :snips fns# :fns :as queries#} (congraph.core/load-queries ~@filenames)]
     (doseq [[id# {fn# :fn meta# :meta}] snips#]
       (conman.core/intern-fn *ns* id# meta# fn#))
     (doseq [[id# {query# :fn meta# :meta}] fns#]
       (conman.core/intern-fn *ns* id# meta#
                              (fn f#
                                ([] (query# ~conn {}))
                                ([params#] (query# ~conn params#))
                                ([conn# params# & args#] (apply query# conn# params# args#)))))
     queries#))
