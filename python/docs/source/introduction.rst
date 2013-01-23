Introduction
=============
Wormtable is a write-once-read-many datastructure to hold large scale 
tabular data. It is designed to provide an efficient means of storing,
searching and retreiving static data.
Tables are arranged in columns and rows: each row 
consists of a set of typed values stored in a packed binary 
format in columns which can be 
indexed and retrieved individually or in groups.
Wormtable uses Berkeley DB for storing rows and for indexing, 
and so is limited in practical terms only by the amount of 
available storage.


