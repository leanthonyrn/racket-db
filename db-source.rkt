#lang racket
#|
A Database is a: 
(database [hashof Table])
with keys representing name of the table

A Table is a:
(table [listof Symbol]
       [vectorof Record]))
with the symbols being field names
field names in Fields MUST match the field names. 

a Field is a:
(field [hashof datum])
keys are names of each field
|#

(struct database (raw))
(struct table (fields raw))
(struct field (raw))