#lang racket
(provide (all-defined-out))

(define (hash!-set hash key val)
  (let ([h (hash-copy hash)])
    (hash-set! hash key val)
    h))