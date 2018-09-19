;;A quick namespace for exploring ideas about using
;;the POI libs for munging PPTX presentations programatically.
(ns proc.powerpoint
  (:require 
             [clojure.java [io :as io]]
             [proc.eutil :as eutil]
             [spork.util [io :refer [list-files fpath fname fext]]])
  (:import [org.apache.poi.xslf.usermodel 
            XMLSlideShow XSLFSlide XSLFPictureData XSLFTheme
            XSLFSlideLayout XSLFSlideMaster SlideLayout XSLFPictureShape] ;any more?
           [java.awt Rectangle]))
              ;[java.io ;maybe unnecessary, used in demo.
            ; FileInputStream FileOutputStream File]
            

;;TODO: Fix reflection warnings.
;; Added wrapper functions to fix reflection warnings
(set! *warn-on-reflection* false)
;;constants and such.
;;==================
;;A map of typename to PictureData enumerated type values (ints),
;;scrapped programtically via reflection.
(def picturedata-types
  (into {} (eutil/get-enums XSLFPictureData PictureType)))

(defn get-picturedata-type
  "Returns java XSLFPictureData.PictureType (int)  from string type-name"
  [type-name] ;; String -> Int
  (if (integer? type-name) type-name
      (eutil/get-or-err picturedata-types type-name)))

;note: doing this programatically, ended up catching a
;;fat finger mistake in the original map:
;;"MEDIDA_AND_TX"
(def slidelayout-types (into {} (eutil/get-constants SlideLayout)))

(defn ^SlideLayout get-slidelayout-type
  "Returns java SlideLayout obj from type."
  [type-name]
  (eutil/get-or-err slidelayout-types type-name))

;;PPT stuff
;;=========
;; Reads ppt from file and returns a ppt object 
(defn ^XMLSlideShow ->pptx
  "Creates a new Apache POI pptx object.  If a file is specified, 
   coerces the file to a PPTX object."
  ([](XMLSlideShow.))
  ([filename] (XMLSlideShow. (io/input-stream filename))))


(defn ^XSLFSlide ->slide
  "Mutably adds a slide to existing pptx presentation, returns the newly added 
   slide."
  ([^XMLSlideShow ppt] (.createSlide ppt))
  ([^XMLSlideShow ppt layout] (.createSlide ppt (int layout))))

;; Saves ppt obj to file
;;Using with-open to manage resources better...
(defn ^XMLSlideShow save-ppt [^XMLSlideShow ppt filename]
  (with-open [out (io/output-stream filename)]
    (.write ppt out))
  ppt)

;; reads picture data and returns java byte array
(defn ^bytes picture->data [filename] ;; String (filename) -> java byte[]
  (eutil/file->bytes filename))

;;(defn get-picture-data [ppt slide filename & {:keys [format] :or {format "PNG"}}]
;;  (^XSLFPictureData .addPicture ppt (picture->data filename) (get-picturedata-type format))) 

;;given a slide, adds picture file to ppt
(defn ^XSLFSlide add-picture
  [^XMLSlideShow ppt ^XSLFSlide slide filename &
   {:keys [format] :or {format "PNG"}}]
  (let [data (.addPicture ppt
                          ^bytes (picture->data filename)
                          (get-picturedata-type format))]
    (doto slide (.createPicture data)))) ;; returns slide

;; Will have to change order in things are done in when adding a picture and setting its location on the slide ...
(defn get-image-shape [ppt slide filename & {:keys [format] :or {format "PNG"}}]
  (let [data (^XSLFPictureData .addPicture ^XMLSlideShow ppt
                          ^bytes (picture->data filename)
                          (get-picturedata-type format))]
    (.createPicture ^XSLFSlide slide data)))

(defn ^XMLSlideShow add-pictures
  "Given a ppt, creates new slide and adds picture from file for each file"
  [ppt filenames]
  (doseq [file filenames]
    (add-picture ppt (->slide ppt) file))
  ppt) ;; returns pp

;; Wrapper function for getSlideMasters method 
;; Takes a XMLSlideShow ppt and returns an array of XSLFSlideMaster objects
(defn get-slide-masters [^XMLSlideShow ppt]
  (vec (.getSlideMasters ppt)))

;; Wrapper function for getLayout method
;; Takes a XSLFSlideMaster and optional type (int) and returns a XSLFSlideLayout
(defn ^XSLFSlideLayout get-layout [^XSLFSlideMaster slide-master &type]
  (let [type (if type (first type) SlideLayout/TITLE_AND_CONTENT)]
    (.getLayout slide-master type)))

(defn ^XMLSlideShow format-layout
  "Creates a new slide on new-ppt with layout from template file"
  [template-file new-ppt]
  (let [template (->pptx template-file) 
        slide-master  (first (get-slide-masters template))
        layout   (get-layout slide-master SlideLayout/TITLE_AND_CONTENT)
        _        (->slide new-ppt layout)]
    new-ppt))

(defn ^XMLSlideShow format-layout-type
  "Creates a new slide on new-ppt with layout from template file
   where slide type determined by type arg (integer - from SlideLayout vars)"
  [template-file new-ppt type]
  (let [template (->pptx template-file)
        slide-master (first (get-slide-masters  template))
        layout (get-layout slide-master type)
        _      (->slide new-ppt layout)]
    new-ppt))

;; Copy nth slide from source-file and appends it to current-ppt 
(defn ^XSLFSlide copy-remote-slide [source-file n current-ppt]
  (let [template (->pptx source-file)
        info (nth (.getSlides template) n)]
    (doto (->slide current-ppt)
      (.importContent  ^XSLFSlide info)))) ;;returns ->slide

;; Wraper function for getSlideLayouts : takes a XSLFSlide master obj and returns
(defn  ;;array of XSLFSlideLayouts 
  get-slide-layouts [^XSLFSlideMaster m]
  (vec (.getSlideLayouts m)))

;; Wrapper function for get-type : takes a XSLFSlideLayout obj and returns a SlideLayout
(defn ^SlideLayout get-type [^XSLFSlideLayout layout]
  (.getType layout))

(defn print-layouts
  "Prints out all layouts that are available in ppt"
  [ppt] ;; Java ppt obj -> nil (Standard Out) 
  (println "Available slide layouts: ")
  (doseq [m (get-slide-masters ppt)]
    (doseq [l (get-slide-layouts m)]
      (println  (get-type l))))) 

(defn ^XSLFTheme get-theme
  "Returns java theme object from slide, or if a file is 
   specified, from the pptx at the file."
  [tgt] ;; Java slide obj | String -> java theme obj
  (let [^XSLFSlide slide (if (string? tgt)  
                             (->slide (->pptx tgt))
                             tgt)]
    (.getTheme slide)))

;;File filtering and other IO stuff...
;;====================================

(defn png?
  "Returns true if filename is specified as a png file"
  [filename]
  (re-matches #".*png$" #_#"[\d\w_-]*\.png" filename))

(defn file-names [p]
  (map fname (list-files p)))

(defn find-images
  "Returns a seq of filenames for png files is the current/given directory."
  [& dir] ;; Optional - alternative directory to look in
  (let [dir  (if dir (first dir) ".")]
    (filter png? (file-names dir))))

(defn filter-filetype
  "Returns a seq of filenames for files of given type in current/given directory"
  [type & dir] ;; String (file type), Optional alternative directory
  (let [dir  (if dir (first dir) ".")]
    (filter #(re-matches (re-pattern (str "[\\d\\w_-]*." type)) %)
            (file-names  dir))))

 
 ;; prints the position of shapes on each slide
(defn print-anchors [^XMLSlideShow ppt] ;; used to copy the shapes positions from an existing ppt
  (doseq [^XSLFSlide slide (.getSlides ppt)]
    (println "\nNEW SLIDE \n")
    (doseq [^XSLFPictureShape sh ( .getShapes slide)]
      (println (type sh))
      (println (^Rectangle .getAnchor sh)))))

(def layout-map ;; map stores positions and sizes for each image to be put onto slides (only works for 1,2,3, or 4 images)
  ;; x y Width Height 
  {1 [(Rectangle. 124 126 476 358)]
   2 [(Rectangle. 36 184 318 238) (Rectangle. 366 184 318 238)]
   3 [(Rectangle. 36 120 318 180) (Rectangle. 366 120 318 180) (Rectangle. 201 324 318 180)]
   4 [(Rectangle. 36 120 318 180) (Rectangle. 366 120 318 180) (Rectangle. 36 324 318 180) (Rectangle. 366 324 318 180)]})
 
(def rectangles {11 (Rectangle. 124 126 476 358)
                 21 (Rectangle. 36 184 318 238)
                 22 (Rectangle. 336 184 318 238)
                 31 (Rectangle. 36 120 318 180) 
                 32 (Rectangle. 366 120 318 180)
                 33 (Rectangle. 201 324 318 180)
                 41 (Rectangle. 36 120 318 180)
                 42 (Rectangle. 366 120 318 180) 
                 43 (Rectangle. 36 324 318 180) 
                 44 (Rectangle. 366 324 318 180)})
           
(def positions {"Middle" 11
                 "Top" 21
                 "Bottom" 22
                 "Top A" 31 
                 "Top B" 32
                 "Bottom Middle" 33
                 "Top Left" 41
                 "Top Right" 42
                 "Bottom Left" 43
                 "Bottom Right" 44})

(defn set-positions [^clojure.lang.LazySeq images & {:keys [layout-map] :or {layout-map layout-map}}] ;; Sets the positions of the images (at this point images are already linked to slide)
  (doseq [^XSLFPictureShape i images :let [index (.indexOf images i)]]
    (.setAnchor i  (nth (get layout-map (count images)) index))))

;; Adds picture to power point at position pos, where pos is a rectangle object
(defn add-image-to-ppt [ppt slide filename pos & {:keys [format tslide] :or {format "PNG" tslide nil}}]
  (let [_ (when tslide (.importContent ^XSLFSlide slide ^XSLFSlide tslide))
        data (^XSLFPictureData .addPicture ^XMLSlideShow ppt
              ^bytes (picture->data filename) (get-picturedata-type format))
        pic-shape (.createPicture ^XSLFSlide slide data)]
    (.setAnchor pic-shape pos)
    pic-shape))

(comment
  (defn prep-images
    "get a sequence of image filepaths from multiple directories"
    [roots num-per-slide img-finder] 
    (let [images (img-finder roots)
          r (- (count images) (rem (count images) num-per-slide))]
      (if (zero? r) (partition num-per-slide images) (conj (partition num-per-slide images) (drop r images))))))

;;layout is seq of rectangle objects
;;if filenames is not divisible by layout, will leave out remainder of filenames
;;returns ppt, need to reduce into to force for seq to evaluate, otherwise will result in empty file if never evaluated
(defn add-images-to-ppt [ppt filenames layout & {:keys [tslide] :or {tslide nil}}]
  (let [part-imgs (map #(zipmap % layout) (partition (count layout) filenames))
        slides (for [n (range (count part-imgs))] (->slide ppt))
        args (zipmap part-imgs slides)]
    (reduce into [] (for [a (keys args)] (map #(add-image-to-ppt ppt (get args a) % (get a %) tslide) (keys a))))
    ppt))
  
 
;; Adds a new slide with images in the correct layout
(defn slide-with-images [ppt filenames & {:keys [format tslide layout] :or {format "PNG" tslide nil layout layout-map}}]
  (let [slide (->slide ppt)
        _ (when tslide (.importContent ^XSLFSlide slide ^XSLFSlide tslide)) 
        imgs (for [i filenames] (get-image-shape ppt slide i))]
    (reduce into [] (set-positions imgs :layout-map layout)) slide)) ;; returns new slide with formatted images

;; given a filepath for a .png file, this should make a powerpoint with 5 slides containg with 0 - 4 images on each 
(defn test-slide-layout [filename & ppt] ;; Used to test if functions work
  (let [ppt (if ppt (first ppt) (->pptx)) files [filename filename filename filename]]
    (doseq [x (range 5)]
      (slide-with-images ppt (take x files)))
    (save-ppt ppt "test-output.pptx")))

;;outfile is the filepath for the output (./some-dir/powerpoint.pptx)
;;files are the list of image filepaths
;;temp is the location of the template file
;;   the contents of the template slides will be used for the output file
;;layouts is a nested sequences or rectangles 
;;ex: [[r1 r2] [r3] [] [r4 r5 r6]] (where ri is a rectangle object
;; ->results in ppt with 2 images on first slide with content from first slide of template
;;                       1 image on second slide "..."
;;                       No image on third slide but contentent from template slide 3 coppied
;;                       3 images on fourth slide "..." (layouts and template content repeated if #files > #layouts)
(defn ->format-pptx [outfile files temp layouts]
  (let [temp-slides (vec (.getSlides (->pptx temp)))
        pptx (->pptx temp)]
    (doall (take-while #(not (empty? (:files %))) 
             (iterate (fn [x]
                        (let [n (count (first (:layouts x)))
                              q (count (:files x))]
                          (if (< q n)
                            (slide-with-images pptx (take n (:files x)) :tslide (first (:tslide x)) 
                              :layout {q (drop (- n q) (first (:layouts x)))})
                            (slide-with-images pptx (take n (:files x)) :tslide (first (:tslide x)) :layout {n (first (:layouts x))}))
                          (-> (assoc x :files (drop n (:files x)))
                              (assoc :tslide (drop 1 (:tslide x)))
                              (assoc :layouts (drop 1 (:layouts x))))))
               {:files files :tslide (flatten (repeat (count files) temp-slides)) :layouts (apply concat (repeat (count files) layouts))})))
    (save-ppt pptx outfile)))



