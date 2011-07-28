#lang racket
(require syntax/parse)
(require "db-data.rkt")
(provide whn)
(define-namespace-anchor current)

(define-syntax-class whn
  #:attributes (func)
  (pattern (~or w:whns #;w:and-whns #;w:or-whns)
           #:attr func
           #'w.func))
(define-syntax-class whns
  (pattern (field check)
           #:attr func
           #'(λ (r) (check (hash-ref (record-raw r) field)))))

#;(define-syntax-class and-whns
  #:literals (and)
  (pattern (and w:whn ...)
           #:attr func
           (λ (r) (and (w.func r) ...))))

#;(define-syntax-class or-whns
  #:literals (or)
  (pattern (or w:whn ...)
           #:attr func
           (λ (r) (or ((attribute w.func) r) ...))))