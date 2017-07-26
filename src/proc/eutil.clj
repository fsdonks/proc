;;Dumping ground for many generic utilities,
;;primarily those that help with interop.
(ns proc.eutil
  (:require [clojure [reflect :as reflect]]
            [clojure.java [io :as io]])
  (:import [java.nio.file Files Paths Path]))

;;aux functions...possibly move these to a util namespace.
;;=======================================================
;;use reflection to rip out all the constants into a map for us...
(defn class->constants [cls]
  (->> (:members (reflect/reflect cls))
       (filter #(every? (:flags %) [:public :static :final]))
       (map :name)))

(defn class-name
  "Rips the class name from a class."
  [^java.lang.Class cls]
  (.getName cls))

(defn class-member
  "Aux function to construct an idiomatic clojure symbol for class 
   member access ala cls/memb."
  [cls memb]
  (symbol (str (class-name cls) "/" memb)))

(defn get-constants
  "Returns all the constants, i.e., static fields with capitalized names, i.e.
   enums, from the class.  Caller supplies an key function to generate
   a key for the field."
  ([cls keyf]
   (for [c (class->constants cls)]
     [(keyf c) (eval (class-member cls c))]))
  ([cls] (get-constants cls str)))

(defn camel->under
  "Coerces a CamelCase string to a CAMEL_CASE style java constant fieldname."
  [s]
  (->> (str s)
       (re-seq #"[A-Z]*[^A-Z]+")
       (map clojure.string/upper-case)
       (clojure.string/join "_")))

(defmacro get-enums
  "Given a java class and an enum datatype, such as 
   XSLFPictureData and PictureType, returns a seq of 
   [k v] for each enumerated type, where enum 
   appears as PictureType.k -> v. "
  [cls root]
  (let [tgt (str (camel->under (str root)) "_")]
     `(get-constants ~cls
        (fn [s#] (clojure.string/replace (str s#) ~tgt "")))))


(defn get-or-err
  "Returns exception if k is not in map m."
  [m k]
  (or (get m k)
      (throw (Exception.
              (str [:unnknown-key k])))))

;;Nio stuff
;;=========
(defn file->bytes
  "Slurps all byes from a file immediately, returning a byte array."
  [p]
  (-> (io/file p)
      ^Path (.toPath)
      (Files/readAllBytes)))
