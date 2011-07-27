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
       [listof Record]))
with the symbols being field names
field names in Fields MUST match the field names. 

a Record is a:
(record [hashof datum])
keys are names of each field

a Query is a:
(query [listof Record])
|#

(struct database (name raw) #:transparent)
(struct table (fields [raw #:mutable]) #:transparent)
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

;; Database [Listof fieldnames] tablename [Listof [Record -> Any]] -> Query
;; queries the DB for selected info
(define (select db whats from whens)
  (define tbl (hash-ref from (database-raw db)))
  (query
   (for/list ([rcrd (table-raw tbl)]
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
  
  (query (for*/list ([r1 v1]
                     [r2 v2]
                     #:when (and (equal? (hash-ref (record-raw r1) on) (hash-ref (record-raw r2) on))
                                 (for/and ([f whens])
                                   (and (f r1)
                                        (f r2)))))
           (let ([r2/1 (hash-copy (record-raw r2))])
             (hash-remove! r2/1 on)
             (record (hash!-append (record-raw r1) r2/1))))))

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
    #:literals (insert #;delete create drop #;update)
    [(_ db (create name (fields ...)))
     #'(create db name (list fields ...))]
    [(_ db (drop table))
     #'(drop db table)]
    [(_ db (insert (values ...) table-name))
     #'(insert db table-name (list values ...))]))

;; update

;; Database TableName [Listof values] -> (void)
;; inserts a record in to the given table with the given values
(define (insert db tbl values)
  (let* ([tbl (hash-ref (database-raw db) tbl)]
         [l (table-raw tbl)])
    (set-table-raw! tbl
                    (cons
                     (record (make-hash (for/list ([fname (table-fields tbl)]
                                                   [v values])
                                          (cons fname v)))) l))))

;; delete

;; Database TableName [listof symbols] -> (void)
;; creates a new table in db with the corresponding tables
(define (create db tbl-name fields)
  (hash-set! (database-raw db) tbl-name (table fields (list))))

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
           "(list",@(for/list ([rec (table-raw tbl)])
                      (serialize-record rec))")")))

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
(define-test-suite --querying
  )
(define-test-suite --effecting
  ;; create
  (let ([db (make-empty-database "")])
    (create db 'test '(oh hai there))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (list))))))))
  (let ([db (make-empty-database "")])
    (effect db (create 'test ('oh 'hai 'there)))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (list))))))))
  ;; drop
  (let ([db (make-empty-database "")])
    (create db 'test '(oh hai there))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (list)))))))
    (drop db 'test)
    (check-equal? db (make-empty-database "")))
  (let ([db (make-empty-database "")])
    (create db 'test '(oh hai there))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (list)))))))
    (effect db (drop 'test))
    (check-equal? db (make-empty-database "")))
  ;; insert 
  (let ([db (make-empty-database "")])
    (create db 'test '(oh hai there))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (list)))))))
    (insert db 'test '(1 2 3))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (list (record (make-hash (list (cons 'oh 1) (cons 'hai 2) (cons 'there 3))))))))))))
  (let ([db (make-empty-database "")])
    (create db 'test '(oh hai there))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (list)))))))
    (effect db (insert (1 2 3) 'test))
    (check-equal? db (database "" (make-hash (list (cons 'test (table '(oh hai there) (list (record (make-hash (list (cons 'oh 1) (cons 'hai 2) (cons 'there 3))))))))))))
  
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
                                                          (list))))))))
                (make-hash 
                 (list 
                  (cons "test"
                        (table '(name id)
                               (list))))))
  (check-equal? (string->value (serialize (database
                                           "" 
                                           (make-hash 
                                            (list 
                                             (cons "test"
                                                   (table '(name id)
                                                          (list (record (make-hash (list (cons 'name "spencer")
                                                                                         (cons 'id "000532210"))))))))))))
                (make-hash 
                 (list 
                  (cons "test"
                        (table '(name id)
                               (list (record (make-hash (list (cons 'name "spencer")
                                                              (cons 'id "000532210"))))))))))
  ;; serialize-table
  (check-equal? (string->value (serialize-table (table '(name id)
                                                       (list))))
                (table '(name id)
                       (list)))
  (check-equal? (string->value (serialize-table (table '(name id)
                                                       (list (record (make-hash (list (cons 'name "spencer")
                                                                                      (cons 'id "000532210"))))))))
                (table '(name id)
                       (list (record (make-hash (list (cons 'name "spencer")
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

(run-tests --querying)
(run-tests --effecting)
(run-tests --loading)
(run-tests --serialize)
