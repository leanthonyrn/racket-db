#lang racket
(require syntax/parse)
(provide (all-defined-out))
(define-syntax-class whns
  (pattern (field check)))