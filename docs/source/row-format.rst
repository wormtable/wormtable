.. _row-format-index:

====================
Low level row format
====================

:Release: |version|
:Date: |today|

This page describes the low level details of the binary format 
used to store each row in Wormtable. These details are not important to 
the average programmer who wishes to store data in a table. It may be 
useful to have a basic understanding of the low-level format when 
tuning for performance, and to understand the tradeoffs associated with 
fixed and variable size columns. It should also be possible to 
build a custom interface to Wormtable in another language that supports
Berkeley DB using this data format definition.


-----------
Row storage
-----------

.. image::  ../images/row-format.png
   :align: center 
   :alt: The low-level row storage format 
   :width: 15cm

-------------
Fixed Columns
-------------

-----------------
Variable Columns
-----------------

-----------------------
Data type representaion
-----------------------

*************
Integer data 
*************

*******************
Floating point data 
*******************

**************
Character data 
**************


