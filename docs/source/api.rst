.. _api-index:

=================
API Documentation
=================

:Release: |version|
:Date: |today|

.. module:: wormtable 
    :platform: Unix
    :synopsis: Write-once read-many table for large datasets. 

--------
Overview
--------

This is the API documentation for wormtable. The documentation currently
concentrates the read-API, since the initial release is intended 
primarily for use with VCF data. For details on how to build a 
wormtable from a VCF file see the :ref:`tutorial <tutorial-index>` or the 
documentation for :ref:`vcf2wt-index`.



The :func:`open_table` function returns a :class:`Table` object
opened for reading, and is analogous to the `open` function 
from the Python standard library. For to get the length 
of a table we might do the following::
    
    import wormtable as wt
    t = open_table("example.wt")
    print("example.wt has ", len(t), "rows")
    t.close()

The :class:`Table` class also supports the 
`context manager <http://www.python.org/dev/peps/pep-0343/>`_
protocol, so we can automatically close a table that has 
been opened::

    import wormtable as wt
    with open_table("example.wt") as t:
        print("example.wt has ", len(t), "rows")


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

    The table class supports the (read-only) Python Sequence protocol. This allows us to
    directly access the rows in a table using the standard Python notation, for example::

        import wormtable as wt
        with open_table("example.wt") as t:
            print("example.wt has ", len(t), "rows")
            print("first row = ", t[0])
            print("last row  = ", t[-1]) 

    Rows are returned as tuples, with values for each column occupying 
    the corresponding position. This is not the recommended interface 
    for retrieving values from a table, however. 
    A more efficient method 
    of iterating over values in a table is to use a :class:`Cursor`.
    
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
