#lang racket
(require rackunit)
(require rackunit/text-ui)
(provide (all-defined-out))
#|
A Database is a: 
(database PathString [hashof Table])
with keys representing name of the table

A Table is a:
(table [listof Symbol]
       [vectorof Record]))
with the symbols being field names
field names in Fields MUST match the field names. 

a Record is a:
(record [hashof datum])
keys are names of each field
|#

(struct database (name raw) #:transparent)
(struct table (fields raw) #:transparent)
(struct record (raw) #:transparent)


;; macros
(define-syntax to-string
  (syntax-rules ()
    [(_ vals ...)
     (with-output-to-string
      (λ ()
        (display vals) ...))]))



;                                          
;                                          
;                                          
;                                          
;   ;;; ;;;            ;      ;;           
;    ;   ;   ;                 ;           
;    ;   ;  ;;;;;    ;;;       ;     ;;;;; 
;    ;   ;   ;         ;       ;    ;    ; 
;    ;   ;   ;         ;       ;     ;;;;  
;    ;   ;   ;         ;       ;         ; 
;    ;   ;   ;   ;     ;       ;    ;    ; 
;     ;;;     ;;;    ;;;;;   ;;;;;  ;;;;;  
;                                          
;                                          
;                                          
;                                          

;; making/updating a DB
;; ---------------------------------------------------------------------------------------------------

;; PathString -> Database
;; loads a database
(define (load-database path)
  (with-handlers ([exn:fail:filesystem? (λ (e) e)])
    (let ([in (open-input-file path)])
      (database path
                (string->values (port->string in))))))
    


;; String -> Datum
;; reads and converts a string
(define-namespace-anchor current)
(define (string->value str)
  (parameterize ([current-namespace 
                  (namespace-anchor->namespace current)])
    (eval (read (open-input-string str)))))


;; saving a DB
;; ---------------------------------------------------------------------------------------------------

;; database -> (void)
;; writes a database
(define (save-database db)
  (with-output-to-file (database-name db)
    (λ () (display (serialize db)))
    #:exists 'replace))

;; database -> string
;; converts the database to a string that when used as the arg to (eval (read (string->port str))) would result in the same raw db
(define (serialize db)
  (to-string
   `(make-hash "(list"
               ,@(for/list ([(tbl-name raw-tbl) (database-raw db)])
                   (string-append "(cons" "\"" (to-string tbl-name) "\"" (serialize-table raw-tbl) ")"))
               ")")))

;; table -> string 
;; converts the table to a string that when used as the arg to (eval (read (string->port str))) would result in the same raw table
(define (serialize-table tbl)
  (to-string 
   `(table "'",(table-fields tbl)
           (vector ,@(for/list ([rec (table-raw tbl)])
                       (serialize-record rec))))))

;; record -> string
;; converts the record to a string that when used as the arg to (eval (read (string->port str))) would result in the same raw record
(define (serialize-record rec)
  (to-string
   `(record 
     (make-hash "'"
                ,(for/list ([(field-name value) (record-raw rec)])
                   (cons field-name (serialize-datum value)))))))

;; datum -> string
;; converts a datum to a read-friendly string
(define (serialize-datum dtm)
  (with-output-to-string
   (λ ()
     (print dtm))))



;                                          
;                                          
;                                          
;                                          
;   ;;;;;;; ;;;;;;   ;;; ;  ;;;;;;;  ;;; ; 
;   ;  ;  ;  ;   ;  ;   ;;  ;  ;  ; ;   ;; 
;      ;     ; ;    ;          ;    ;      
;      ;     ;;;     ;;;;      ;     ;;;;  
;      ;     ; ;         ;     ;         ; 
;      ;     ;           ;     ;         ; 
;      ;     ;   ;  ;;   ;     ;    ;;   ; 
;     ;;;   ;;;;;;  ; ;;;     ;;;   ; ;;;  
;                                          
;                                          
;                                          
;                                          

(define-test-suite --serialize
  ;; serialize
  (check-equal? (string->value (serialize (database "" (make-hash))))
                (make-hash (list)))
  
  (check-equal? (string->value (serialize (database
                                             "" 
                                             (make-hash 
                                              (list 
                                               (cons "test"
                                                     (table '(name id)
                                                            (vector))))))))
                (make-hash 
                 (list 
                  (cons "test"
                        (table '(name id)
                               (vector))))))
  (check-equal? (string->value (serialize (database
                                             "" 
                                             (make-hash 
                                              (list 
                                               (cons "test"
                                                     (table '(name id)
                                                            (vector (record (make-hash (list (cons 'name "spencer")
                                                                                             (cons 'id "000532210"))))))))))))
                (make-hash 
                 (list 
                  (cons "test"
                        (table '(name id)
                               (vector (record (make-hash (list (cons 'name "spencer")
                                                                (cons 'id "000532210"))))))))))
  ;; serialize-table
  (check-equal? (string->value (serialize-table (table '(name id)
                                                         (vector))))
                (table '(name id)
                       (vector)))
  (check-equal? (string->value (serialize-table (table '(name id)
                                                         (vector (record (make-hash (list (cons 'name "spencer")
                                                                                          (cons 'id "000532210"))))))))
                (table '(name id)
                       (vector (record (make-hash (list (cons 'name "spencer")
                                                        (cons 'id "000532210")))))))
  ;; serialize-record
  (check-equal? (string->value (serialize-record (record (make-hash (list (cons 'name "spencer")
                                                                            (cons 'id "000532210"))))))
                (record (make-hash (list (cons 'name "spencer")
                                         (cons 'id "000532210")))))
  ;; serialize-datum
  (check-equal? (string->value (serialize-datum "spencer"))
                "spencer")
  (check-equal? (string->value (serialize-datum 1223))
                1223)
  (check-equal? (string->value (serialize-datum #f))
                #f)
  (check-equal? (string->value (serialize-datum 'yes?))
                'yes?)
  )

(run-tests --serialize)
