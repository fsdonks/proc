;;Added this to account for the data-driven constants file
;;that may contain sensitive data.  In reality, require that
;;this file be loaded at run-time, rather than baking our
;;data into source.  For purposes of shimming the legacy
;;implementation, this ns provides empty stubs so that
;;other namespaces at least compile.
(ns proc.constants)


;;Stubs.
(def excluded-phases :excluded-phases-stub)
(def starts :starts-stub)
(def somestats :somestats-stub)
(def behruns :behruns-stub)
(def bundle-1 :bundle-1-stub)
