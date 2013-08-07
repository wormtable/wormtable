.. _api-index:

=================
API Documentation
=================

:Release: |version|
:Date: |today|


This is the API documentation for wormtable. The documentation currently
concentrates the read-API, since the initial release is intended 
primarily for use with VCF data. For details on how to build a 
wormtable from a VCF file see the :ref:`tutorial <tutorial-index>`.

In the :ref:`api-examples` section we take an informal tour of the API using
a small example table. The :ref:`api-reference` section provides concrete 
API documentation for the :mod:`wormtable` module.

.. _api-examples:

---------
Examples
---------

.. py:currentmodule:: wormtable 

To illustrate the :mod:`wormtable` API, we use a wormtable `pythons.wt` which contains
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

This table consists of six columns: the name of the Python, the year they were 
born and the number of entries in `IMDB <http://www.imdb.com>`_ they have under these
headings as of 2013.

######
Tables
######

The :func:`open_table` function returns a :class:`Table` object
opened for reading, and is analogous to the :func:`open` function 
from the Python standard library. So, to open our `pythons.wt` table 
for reading, we might do the following::
    
    >>> import wormtable as wt
    >>> t = wt.open_table("pythons.wt")
    >>> len(t)
    6

Tables that are opened should be closed when they are no longer needed. 
This is done using the :meth:`Table.close` method, again analogous
to Python file handling. Trying to access a closed table results in 
an error::

    >>> t.close()
    >>> len(t)
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "./wormtable.py", line 637, in __len__
        self.verify_open()
      File "./wormtable.py", line 447, in verify_open
        raise ValueError("Database must be opened") 
    ValueError: Database must be opened


The :class:`Table` class also supports the 
`context manager <http://www.python.org/dev/peps/pep-0343/>`_
protocol, so we can automatically close a table that has 
been opened::

    with wt.open_table("pythons.wt") as t:
        print(len(t))
    # t is now closed and cannot be accessed

The :class:`Table` class supports the read-only Python sequence 
protocol, and so tables can be treated like a two-dimensional list in 
many ways. For example::

    >>> t = wt.open_table("pythons.wt")
    >>> t[0]
    (0, b'John Cleese', 1939, 60, 127, 0, 43)
    >>> t[-1]
    (5, b'Graham Chapman', 1941, 46, 24, 0, 2)
    >>> t[4:]
    [(4, b'Michael Palin', 1943, 58, 56, 0, 1), (5, b'Graham Chapman', 1941, 46, 24, 0, 2)]

Rows are returned as tuples, with values for each column occupying 
the corresponding position. Each table consists of a fixed number of columns, which 
describe the size and type of the data in the column. The :class:`Column` class 
has some methods to query these types and sizes, and these are accessed 
either via the :meth:`Table.columns` method, or the :meth:`Table.get_column`
method::

    >>> [c.get_name() for c in t.columns()]
    ['row_id', 'name', 'born', 'writer', 'actor', 'director', 'producer']
    >>> c = t.get_column("born")
    >>> (c.get_type_name(), c.get_element_size())
    ('uint', 2)

This tells us that the ``born`` column holds unsigned integer data with 
an element size of 2, and so it can store values from 0 to 65534. See 
:ref:`data-types-index` for details on the various data types 
and sizes supported by wormtable.

The first column in every wormtable is an unsigned integer column, 
called ``row_id``. This is the column used to index rows,
and the size of this column determines the number of rows that can be 
stored in the table. As a result, we always have ``t[j][0] == j``::

    >>> [t[j][0] for j in range(len(t))]
    [0, 1, 2, 3, 4, 5]


#######
Cursors
#######

Suppose we are only interested in the name and the birth year of the pythons. We could 
do something like::

    >>> t = wt.open_table("pythons.wt")
    >>> [(r[1], r[2]) for r in t]
    [(b'John Cleese', 1939), (b'Terry Gilliam', 1940), (b'Eric Idle', 1943), (b'Terry Jones', 1942), (b'Michael Palin', 1943), (b'Graham Chapman', 1941)]

This is very inefficient if we have a large number of columns, because :mod:`wormtable`
must build a tuple containing all of the columns in each row, even though most of this will
not be used. It is also inconvenient: we must remember that the `name` column is in position 
1, and the `born` column is in position 2. 

A much more efficient and convenient approach is to use a *cursor*. Cursors
provide a simple means of iterating over rows, and retrieving values for a given 
set of columns. Repeating the example above::

    >>> [r for r in t.cursor(["name", "born"])]
    [(b'John Cleese', 1939), (b'Terry Gilliam', 1940), (b'Eric Idle', 1943), (b'Terry Jones', 1942), (b'Michael Palin', 1943), (b'Graham Chapman', 1941)]

The :meth:`Table.cursor` method returns an iterator over the rows in a table 
for a list of columns. (Cursors are intended to
be used over very large datasets, and so we would not usually construct a list of the rows.)
The :meth:`Table.cursor` method also provides a way to restrict the rows
retrieved from the table using the *start* and *stop* arguments (this is analogous to the 
built in :func:`range` function).
For example, to only retrieve rows 1 to 3, we would
do the following (rows are zero-indexed in wormtable)::

    >>> [r for r in t.cursor(["name", "born"], start=1, stop=4)]
    [(b'Terry Gilliam', 1940), (b'Eric Idle', 1943), (b'Terry Jones', 1942)]

Note that *start* is **inclusive** and *stop* is **exclusive**.

##############
Simple Indexes
##############

Suppose we wished to rank the Monty Python team in terms of writing credits from IMDB. We could
simply retrieve the columns that we are interested in  and sort them in terms of the ``writer``
column using the built in :func:`sorted` function. This does not work very well, however, if we
have millions of rows in our table. It is very slow, and may not even be possible if there are
too many rows to fit in memory.

An *index* in wormtable is a persistent sorting of a table with respect to a given column
(or list of columns, as we see in the `Compound Indexes`_ section). Indexes are extremely useful, and 
can be used to make many different operations more efficient. Each index has a *name*, which 
is its unique identifier. Indexes are created using the
``wtadmin`` command line tool.

To open an index on a table, we use the :meth:`Table.open_index`
method. For example, to open an index called ``writer``, we might use::

    >>> i = t.open_index("writer")

The :meth:`Table.open_index` method is directly analogous to the :func:`open_table` function 
used to open tables. Indexes should be closed after use, like tables, and also support
the `context manager <http://www.python.org/dev/peps/pep-0343/>`_ protocol to 
automatically close indexes::

    with t.open_index("writer") as i:
        print(i.max_key())
    # Index i is now closed and cannot be accessed

Indexes sort the *keys* in the columns of interest, and map these keys to the rows
of the table where they are found. To get the minimum and maximum keys from the
index, we use the :meth:`Index.min_key` and :meth:`Index.max_key` methods::

    >>> i = t.open_index("writer")
    >>> (i.min_key(), i.max_key())
    (25, 60)

This tells us that the least productive Python has 25 writing credits on 
IMDB, and the most has 60. This does not tell us *who* they are though. To 
get information about other columns, we must use a *cursor*::

    >>> for r in i.cursor(["name", "writer"]):
    ...     print(r)
    ... 
    (b'Terry Gilliam', 25)
    (b'Eric Idle', 38)
    (b'Graham Chapman', 46)
    (b'Terry Jones', 50)
    (b'Michael Palin', 58)
    (b'John Cleese', 60)

Just like the :meth:`Table.cursor` method, :meth:`Index.cursor` iterates over 
rows in the table for a selection of columns. The difference between the two 
is that the *order* in which the rows are returned is the order defined by 
the index. The *start* and *stop* arguments to the function are also now 
in terms of index keys, and not row positions. This gives us a very flexible 
method of obtaining rows from the table based on the *values* that they 
contain.  For example, if we are only interested in the Pythons who have between 30 
(inclusive) and 50 (exclusive) writing credits, we can write::

    >>> for r in i.cursor(["name", "writer"], start=30, stop=50):
    ...     print(r)
    ... 
    (b'Eric Idle', 38)
    (b'Graham Chapman', 46)

##############
Index Counters
##############

To find out the number of rows in a table correspond to a given index key, we use 
a Counter object. This is closely modelled on a the :class:`collections.Counter`
class; it is a mapping from keys to the number of rows in the table containing 
this key. For example, if we make an index on the ``director`` Column::

    >>> i = t.open_index("director")
    >>> c = i.counter()
    >>> for k, v in c.items(): 
    ...     print(k, "->",  v)
    ... 
    0 -> 3
    7 -> 1
    16 -> 1
    18 -> 1

This shows that there are 3 Pythons who have directed 0 films,
and the three others have directed 7, 16 and 18 respectively.
Counters implement the read-only Python mapping protocol, and so can be treated 
very much like a dictionary::

    >>> c[0]
    3
    >>> c[1]
    0
    >>> len(c)
    4


################
Compound Indexes
################

Wormtable also supports indexes over more than one column. These differ from simple 
indexes in that the keys for each index are constructed by concatenating the
values from the constituent columns, in the order that they are specified. For example, 
we can make an index on the columns ``director`` and ``producer``, which we call
``director+producer``::

    >>> i = t.open_index("director+producer")
    >>> for r in i.cursor(["name", "director", "producer"]):
    ...     print(r)
    ... 
    (b'Michael Palin', 0, 1)
    (b'Graham Chapman', 0, 2)
    (b'John Cleese', 0, 43)
    (b'Eric Idle', 7, 5)
    (b'Terry Jones', 16, 1)
    (b'Terry Gilliam', 18, 8)
    
This lists the rows in the order defined by the index. Keys are sorted lexicographically,
so that we sort on the first column first, and if there are duplicate values for the first
column we then sort on the second column. Here, for example, we have Michael Palin, Graham 
Chapman and John Cleese have all directed 0 films. But since this is a compound index, 
we then sort on the producer column, giving the ordering that we see.

Since keys now contain values from multiple columns, the :meth:`Index.min_key` and 
:meth:`Index.max_key` now return tuples::

    >>> i.min_key()
    (0, 1)
    >>> i.max_key()
    (18, 8)

These are also more flexible now, however, as we can get the minimum and maximum keys 
with a given prefix::

    >>> i.min_key(7)
    (7, 5)
    >>> i.max_key(0)
    (0, 43)

The *start* and *stop* arguments to the :meth:`Index.cursor` method 
also support this flexible key prefixing. Suppose we wish to 
find all the Pythons with at least 7 directorial credits::

    >>> [r for r in i.cursor(["name", "director", "producer"], start=7)]
    [(b'Eric Idle', 7, 5), (b'Terry Jones', 16, 1), (b'Terry Gilliam', 18, 8)]

We get the same answer if we specify 5 or less for the ``producer`` column::

    >>> [r for r in i.cursor(["name", "director", "producer"], start=(7, 0))]
    [(b'Eric Idle', 7, 5), (b'Terry Jones', 16, 1), (b'Terry Gilliam', 18, 8)]

But, we lose Eric Idle if we require 6 or more production credits::

    >>> [r for r in i.cursor(["name", "director", "producer"], start=(7, 6))]
    [(b'Terry Jones', 16, 1), (b'Terry Gilliam', 18, 8)]


.. _api-reference:

----------------
Module reference
----------------

.. module:: wormtable 
    :platform: Unix
    :synopsis: Write-once read-many table for large datasets. 

.. autofunction:: open_table


####################
:class:`Table` class
####################

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
   
    .. automethod:: columns

    .. automethod:: get_column


####################
:class:`Index` class
####################

.. class:: Index

    Indexes define a *sorting order* of the rows in a table. An index over a set
    of columns creates a sorting order over the table by concatenating the values 
    from the columns in question together (the *keys*) and storing the mapping 
    of these keys to the rows in the table in which the key occurs.

    .. automethod:: cursor
    
    .. automethod:: Index.open

    .. automethod:: Index.close

    .. automethod:: Index.min_key

    .. automethod:: Index.max_key

    .. automethod:: Index.keys
    
    .. automethod:: Index.counter

#####################
:class:`Column` class
#####################

.. class:: Column
    
    Columns define the storage types for values within a table.

    .. automethod:: get_name 
    
    .. automethod:: get_description

    .. automethod:: get_type

    .. automethod:: get_type_name

    .. automethod:: get_element_size
    
    .. automethod:: get_num_elements

