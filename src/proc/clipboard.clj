"Namespace for function that interact with the System's native clipboard for copying and pasting file and images"
(ns proc.clipboard
  ;;(:require [proc.charts])
  (:import
   [java.awt Toolkit]
   [java.awt.datatransfer Transferable DataFlavor]
   [java.io File])) 

;; creates a temp file which is deleted on exit
(defn temp-file [filename]
  (let [f (java.io.File. filename)]
    (.deleteOnExit f) f))

;; creates new 'transfer proxy' with files added to data to be copied
(defn new-file-trans [files]
  ;;(let [ft (exercises.clipboard.clip.)]
  (let [ft (proxy [java.awt.datatransfer.Transferable] []
                 (getTransferDataFlavors [] (into-array DataFlavor [DataFlavor/javaFileListFlavor]))
                 (isDataFlavorSupported [flavor] true)
                 (getTransferData [flavor] files))] ft))

;; Copies all files to the system clipboard to be used by external programs 
(defn copy-file-to-clip [files]
  (let [cb (.getSystemClipboard (java.awt.Toolkit/getDefaultToolkit))]
    (.setContents cb (new-file-trans files)
                  (proxy [java.awt.datatransfer.ClipboardOwner] []
                    (lostOwnership [cb conts]
                      (print ""))))))
                                        ;(println "Proxy override - lost ownership"))))))

;; Creates a temp file from buffered image and copies it to the system clipboard
(defn image-to-temp [buff]
  (let [filename (str ".\\resources\\~" (gensym) ".jpg")]
    (javax.imageio.ImageIO/write buff, "jpg", (File. filename))
    filename))

;; Copies all buffered images to system clipboard (iterates over image-to-temp)
(defn copy-images-to-clip [imgs]
  (copy-file-to-clip (for [i imgs] (image-to-temp i))))

;; MOVED TO CHARTS NS 
;; Listens for when "ctrl + c" is pressed and does function (println) when true; only when frame is in focus 
(comment
(defn listen-for-keys [chart title frame] 
  (.addKeyListener frame (proxy [java.awt.event.KeyListener] []
                           (keyPressed [e]
                             (when (and (.isControlDown e) (= java.awt.event.KeyEvent/VK_C (.getKeyCode e)))
                               (println "KEY PRESSED")))))) ;; change to save fill/dwell charts to file then copy to clipboard
)
    

