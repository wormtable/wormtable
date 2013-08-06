.. _introduction-index:

=============
Introduction
=============

:Release: |version|
:Date: |today|


Wormtable is a write-once-read-many data structure to hold large scale 
tabular data. It is designed to provide an efficient means of storing,
searching and retrieving static data.  Tables are arranged in columns and rows: each row 
consists of a set of typed values stored in columns. Columns can be  
indexed individually or in groups. Wormtable uses Berkeley DB to index
tables.

Wormtable has several key goals:

Performance
     To provide high performance access to data from a large number of 
     rows, wormtable encodes data in a compact binary format that can 
     be quickly converted to native types. The core of wormtable is 
     written in C for efficiency.

Scalability
    Berkeley DB provides us with 
    world-class database technology, ensuring that we can continue 
    to scale as datasets get larger and larger. 
    The maximum number of rows in a wormtable is 
    2^64 - 1 and the maximum file size is limited only by available 
    storage.

Portability
    Wormtable can be deployed on any Unix system supporting Berkeley DB, and 
    is therefore extremely portable. It has been tested on big and little endian 
    systems, with 32 and 64 bit word sizes. Wormtable files can be written 
    or read on any machine, regardless of operating system, word size or 
    endianness.
