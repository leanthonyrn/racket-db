#lang racket
(require (for-syntax syntax/parse))
(require rackunit)
(require rackunit/text-ui)
(require "hash-remaps.rkt")
(require (for-syntax "stx-classes.rkt"))
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

a Query is a:
(query [vectorof Record])
|#

(struct database (name raw) #:transparent)
(struct table (fields raw) #:transparent)
(struct record (raw) #:transparent)
(struct query (raw) #:transparent)

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
;     ;;    ;;;;;    ;;;;; 
;      ;     ;   ;     ;   
;     ; ;    ;   ;     ;   
;     ; ;    ;   ;     ;   
;     ; ;    ;;;;      ;   
;     ;;;    ;         ;   
;    ;   ;   ;         ;   
;   ;;; ;;; ;;;      ;;;;; 
;                          
;                          
;                          
;                          
                                       


;; query
;; ---------------------------------------------------------------------------------------------------

(define-syntax (query-mac stx)
  (syntax-parse stx
    #:literals (select join)
    [(_ db (select (whats ...) from w:whns ...))
     #'(select db 
               (list whats ...) 
               from 
               (list (λ (r) (w.check (hash-ref (record-raw r) w.field))) ...))]
    [(_ db (join (from1 from2) in w:whns ...))
     #'(join db from1 from1 in (list (λ (r) (w.check (hash-ref (record-raw r) w.field))) ...))]))

;; select

;; Database [Listof fieldnames] tablename [Listof [Record -> Any]] -> Query
;; queries the DB for selected info
(define (select db whats from whens)
  (define tbl (hash-ref from (database-raw db)))
  (query
   (for/vector ([rcrd (table-raw tbl)]
                #:when (for/and ([f whens])
                         (f rcrd)))
     (record 
      (make-hash (for/list ([(key val) (record-raw rcrd)]
                            #:when (or (empty? whats (member key whats))))
                   (cons key val)))))))

;; Database [Listof Tablenames] Fieldname [Listof [Record -> Any]] -> Query
;; joins two tables in the DB
(define (join db from1 from2 on whens)
  (define v1 (table-raw (hash-ref (database-raw db) from1)))
  (define v2 (table-raw (hash-ref (database-raw db) from2)))
  
  (query (for*/vector ([r1 v1]
                       [r2 v2]
                       #:when (and (equal? (hash-ref (record-raw r1) on) (hash-ref (record-raw r2) on))
                                   (for/and ([f whens])
                                     (f r1)
                                     (f r2))))
           (let ([r2.1 (hash-copy (record-raw r2))])
             (hash-remove! r2.1 on)
           (record (hash!-append (record-raw r1) r2.1))))))
                      
;; hash hash -> hash
;; appends two hashes
(define (hash!-append h1 h2)
  (define ret (hash-copy h1))
  (for ([(key val) h2])
    (hash-set! ret key val)))
  
;; effect (insert,delete, create)
;; ---------------------------------------------------------------------------------------------------
(define-syntax (effect stx)
  (syntax-parse stx
    #:literals (#;insert #;delete create drop #;update)
    [(_ db (create name (fields ...)))
     #'(create db name (list fields ...))]
    [(_ db (drop table))
     #'(drop db table)]))

;; update

;; insert

;; delete

;; Database TableName [listof symbols] -> (void)
;; creates a new table in db with the corresponding tables
(define (create db tbl-name fields)
  (hash-set! (database-raw db) tbl-name (table fields (vector))))

;; Database TableName -> (void)
;; drops the given table
(define (drop db tbl-name)
  (hash-remove! (database-raw db) tbl-name))
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
;; creates a new unsaved database
(define (make-empty-database path)
  (database path (make-hash)))

;; PathString -> Database
;; loads a database
(define (load-database path)
  (with-handlers ([exn:fail:filesystem? (λ (e) e)])
    (database path
              (string->value (port->string (open-input-file path))))))
    


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
(define-test-suite --effecting
  ;; create
  (let ([db (make-empty-database "")])
    (create db 'test '(oh hai there))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (vector))))))))
  (let ([db (make-empty-database "")])
    (effect db (create 'test ('oh 'hai 'there)))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (vector))))))))
  ;; drop
  (let ([db (make-empty-database "")])
    (create db 'test '(oh hai there))
    (drop db 'test)
    (check-equal? db (make-empty-database "")))
  (let ([db (make-empty-database "")])
    (create db 'test '(oh hai there))
    (effect db (drop 'test))
    (check-equal? db (make-empty-database "")))
  )
(define-test-suite --loading
  ;; string->value
  (check-equal? (string->value "(cons 1 2)")
                (cons 1 2))
  (check-equal? (string->value "1")
                1)
  (check-exn exn:fail:read? (λ () (string->value "(cons 1 2")))
  (check-equal? (string->value "(database \"\" (make-hash))")
                (database "" (make-hash)))
  
  )
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

(run-tests --effecting)
(run-tests --loading)
(run-tests --serialize)
