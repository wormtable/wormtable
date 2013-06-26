.. _api-index:

=================
API Documentation
=================

:Release: |version|
:Date: |today|

.. module:: wormtable 
    :platform: Unix
    :synopsis: Write-once read-many table for large datasets. 

This is the API documentation for wormtable. The documentation currently
concentrates the read-API, since the initial release is intended 
primarily for use with VCF data. For details on how to build a 
wormtable from a VCF file see the :ref:`tutorial <tutorial-index>` or the 
documentation for :ref:`vcf2wt-index`.


---------
Reference
---------

.. autofunction:: open_table

.. class:: Table

    The table class represents a wormtable located in a *home directory*. The home directory 
    for a table stores the Berkeley DB databases used to store the rows and indexes, along 
    with some metadata to describe these tables. The files within a home directory are not 
    intended to be accessed directly; modifications to a table should be made through 
    this API only.

   
    .. automethod:: cursor

    .. automethod:: open_index

    .. automethod:: open
    
    .. automethod:: close 

    .. automethod:: set_cache_size 
   
    .. automethod:: columns

    .. automethod:: get_column



.. class:: Cursor


    Cursors provide an efficient means of iterating over the rows in a table, 
    retreiving a subset of the columns in the row. This is much more efficient 
    that iterating over the table directly and retreiving the values of 
    interest from the tuples returned because only the values that we are 
    interested in are converted into Python values and passed back. 

    Cursors are instantiated using the :meth:`Table.cursor` method
    which provide the interface for choosing the columns that we
    are interested in. Cursors also provide methods to set the 
    maximum and minimum values of the row keys we wish to examine.

    .. automethod:: Cursor.set_min

    .. automethod:: Cursor.set_max

.. class:: Index

    Indexes define a *sorting order* of the rows in a table. An index over a set
    of columns creates a sorting order over the table by concatenating the values 
    from the columns in question together (the *keys*) and storing the mapping 
    of these keys to the row in the table in which the key occurs.

    .. automethod:: Index.open

    .. automethod:: Index.close

    .. automethod:: Index.get_min

    .. automethod:: Index.get_max

    .. automethod:: Index.counter


.. class:: Column
    
    Columns define the storage types for values within a table.

    .. automethod:: get_name 


---------
Examples
---------

To illustrate the API above, we use a wormtable `pythons.wt` which contains
the following data:

==============      ====    ======  =====   ========    ========
name                born    writer  actor   director    producer 
==============      ====    ======  =====   ========    ========
John Cleese         1939    60      127     0           43  
Terry Gilliam       1940    25      24      18          8   
Eric Idle           1943    38      74      7           5   
Terry Jones         1942    50      49      16          1   
Michael Palin       1943    58      56      0           1   
Graham Chapman      1941    46      24      0           2   
==============      ====    ======  =====   ========    ========

This table consists of three columns: the name of the Python, the year they were 
born and the number of entries in `IMDB <http://www.imdb.com>`_ they have under these
headings as of 2013.

The :func:`open_table` function returns a :class:`Table` object
opened for reading, and is analogous to the :func:`open` function 
from the Python standard library. So, to open our Pythons table 
for reading, we might do the following::
    
    >>> import wormtable as wt
    >>> t = wt.open_table("pythons.wt")
    >>> len(t)
    6

The :func:`len` function tells us that there are 6 rows in the `pythons.wt`
table.  The :class:`Table` class supports the read-only Python sequence 
protocol, and so it can be treated like a two-dimensional list in 
many ways. For example::

    >>> t[0]
    (0, b'John Cleese', 1939, 60, 127, 0, 43)
    >>> t[-1]
    (5, b'Graham Chapman', 1941, 46, 24, 0, 2)
    >>> t[4:]
    [(4, b'Michael Palin', 1943, 58, 56, 0, 1), (5, b'Graham Chapman', 1941, 46, 24, 0, 2)]

Rows are returned as tuples, with values for each column occupying 
the corresponding position. This is not the recommended interface 
for retrieving values from a table, however. 
A more efficient method 
of iterating over values in a table is to use a :class:`Cursor`.




The :class:`Table` class also supports the 
`context manager <http://www.python.org/dev/peps/pep-0343/>`_
protocol, so we can automatically close a table that has 
been opened::

    with wt.open_table("sample.wt") as t:
        print("example.wt has ", len(t), "rows")


