#lang racket
(require syntax/parse (for-syntax syntax/parse))
(require rackunit)
(require rackunit/text-ui)
(require "hash-remaps.rkt")
(require "db-data.rkt")
(require (for-syntax "stx-classes.rkt"))
(provide (all-defined-out))

(define-namespace-anchor current)


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
    [(_ db (select (whats ...) from))
     #'(select db 
               (list whats ...) 
               from 
               (const #t))]
    [(_ db (select (whats ...) from w:whn))
     #'(select db 
               (list whats ...) 
               from 
               w.func)]
    [(_ db (join (from1 from2) in))
     #'(join db
             from1 from1
             in 
             (const #t))]
    [(_ db (join (from1 from2) in w:whn))
     #'(join db
             from1 from1
             in 
             (attribute w.func))]))

;; Database [Listof fieldnames] tablename [Record -> Any] -> Query
;; queries the DB for selected info
(define (select db whats from when)
  (define tbl (hash-ref (database-raw db) from))
  (query
   (for/list ([rcrd (table-raw tbl)]
              #:when (when rcrd))
     (record 
      (make-hash (for/list ([(key val) (record-raw rcrd)]
                            #:when (or (empty? whats) (member key whats)))
                   (cons key val)))))))

;; Database [Listof Tablenames] Fieldname [Record -> Any] -> Query
;; joins two tables in the DB
(define (join db from1 from2 on when)
  (define v1 (table-raw (hash-ref (database-raw db) from1)))
  (define v2 (table-raw (hash-ref (database-raw db) from2)))
  
  (query (for*/list ([r1 v1]
                     [r2 v2]
                     #:when (and (equal? (hash-ref (record-raw r1) on) (hash-ref (record-raw r2) on))
                                   (and (when r1)
                                        (when r2))))
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
(define queryable (make-empty-database ""))
(effect queryable (create 'test1 ('a 'b)))
(effect queryable (insert (1 'me) 'test1))
(effect queryable (insert (2 'you) 'test1))
(effect queryable (insert (3 'you) 'test1))
(effect queryable (insert (4 'me) 'test1))
(effect queryable (create 'test2 ('a 'c)))
(effect queryable (insert (1 'you) 'test2))
(effect queryable (insert (2 'me) 'test2))
(effect queryable (insert (3 'me) 'test2))
(effect queryable (insert (4 'you) 'test2))

(define-test-suite --querying
  ;; select
  (check-equal? (select queryable '() 'test1 (const #t))
                (query (table-raw (hash-ref (database-raw queryable) 'test1))))
  (check-equal? (query-mac queryable (select () 'test1))
                (query (table-raw (hash-ref (database-raw queryable) 'test1))))
  (check-equal? (select queryable '(a) 'test1 (const #t))
                (query (list (record (make-hash (list (cons 'a 4))))
                             (record (make-hash (list (cons 'a 3))))
                             (record (make-hash (list (cons 'a 2))))
                             (record (make-hash (list (cons 'a 1)))))))
  (check-equal? (query-mac queryable (select ('a) 'test1))
                (query (list (record (make-hash (list (cons 'a 4))))
                             (record (make-hash (list (cons 'a 3))))
                             (record (make-hash (list (cons 'a 2))))
                             (record (make-hash (list (cons 'a 1)))))))
  (check-equal? (select queryable '(a) 'test1 (λ (r) (= (hash-ref (record-raw r) 'a) 4)))
                (query (list (record (make-hash (list (cons 'a 4)))))))
  (check-equal? (query-mac queryable (select ('a) 'test1 ('a (λ (a) (= a 4)))))
                (query (list (record (make-hash (list (cons 'a 4)))))))
  
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
