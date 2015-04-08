.. _data-storage-index:

============
Data storage
============

:Release: |version|
:Date: |today|

All data stored in a wormtable is *typed*: individual elements are either integers,
floating point values or strings of characters. The advantage of this typing system
is that data is stored in a compact binary format, minimising the space used and
also allowing us to efficiently retrieve values from the table.

This page describes the type and sizes of data that may be stored in columns,
as well as some low-level details about this data storage format. While this
information is not directly relevant to programmers using wormtable, it
can help to understand the various tradeoffs when choosing a particular
type or choosing a variable versus a fixed length column.

.. _data-types-index:

---------------
Column types
---------------

The fundamental storage units of wormtable are *rows* and *columns*,
similar to relational databases. A row is split into a fixed number of
columns, and within a column we store some number of *elements* of a
fixed *type* and *size*. The size of the elements determines the range
of the values that can be stored in a column; for example, an unsigned
integer column with element size 1 can store values from 0 up to
254 only.

The number of elements that is stored in a column is
either fixed or variable. In a column with a fixed number of elements,
space is reserved for all of these elements, and so the total space used
by that column is always *num_elements* * *element_size*.
In variable length columns, we can store
from 0 to 255 elements for ``var(1)`` and from 0 to 65535 in ``var(2)``.

.. _int-types-index:

***************
Integer columns
***************

Integer columns with an element size :math:`n`  are :math:`n` byte signed
integers. They can store values in the range :math:`-2^{8n - 1} + 1 \leq x
\leq 2^{8n - 1} - 1`. Element sizes of :math:`1` up to :math:`8` are supported.

============    ====================    ===================
Element size    Min                     Max
============    ====================    ===================
1               -127                    127
2               -32767                  32767
3               -8388607                8388607
4               -2147483647             2147483647
5               -549755813887           549755813887
6               -140737488355327        140737488355327
7               -36028797018963967      36028797018963967
8               -9223372036854775807    9223372036854775807
============    ====================    ===================

.. _uint-types-index:

************************
Unsigned integer columns
************************
Unsigned integer columns with an element size :math:`n`  are :math:`n` byte
unsigned integers. They can store values in the range :math:`0 \leq x
\leq 2^{8n} - 2`. Element sizes of :math:`1` up to :math:`8` are supported.

============    ====    ===================
Element size    Min     Max
============    ====    ===================
1               0       254
2               0       65534
3               0       16777214
4               0       4294967294
5               0       1099511627774
6               0       281474976710654
7               0       72057594037927934
8               0       18446744073709551614
============    ====    ===================


.. _float-types-index:

**********************
Floating point columns
**********************
Floating point columns represent real numbers using the
`IEEE floating point <https://en.wikipedia.org/wiki/IEEE_floating_point>`_
format. Floating point values that require 2, 4 and 8 bytes of storage
are supported, corresponding to IEEE half, single and double precision,
respectively. Half precision floats are useful for storing
small numbers with low precision, and can lead to substantial savings
(see :ref:`performance-schema` for an example).

============    =======      ===================
Element size    C type       Details
============    =======      ===================
2               N/A          `float 16 <https://en.wikipedia.org/wiki/Half_precision_floating-point_format>`_
4               float        `float 32 <https://en.wikipedia.org/wiki/Single_precision_floating-point_format>`_
8               double       `float 64 <https://en.wikipedia.org/wiki/Double_precision_floating-point_format>`_
============    =======      ===================

*****************
Character columns
*****************

Character columns store strings of bytes, and are treated in a slightly different way to the numeric
columns seen above. For a character column, the `element_size` must be equal to 1, since
only one-byte character sets are supported. A fixed size of column `num_elements` elements can
therefore store strings of length up to `num_elements`. Variable length `var(1)` `char` columns
can store strings of length 0 to 255 bytes, and `var(2)` columns can store strings of
up to 65535 bytes.


----------
Row format
----------

While the low-level details of the binary format used by wormtable are
not of interest to the average programmer, it may be
useful to have a basic understanding of the low-level format when
tuning for performance.

***********
Row storage
***********

Rows hold data from a fixed number of predefined columns, which are together referred
to as a **schema**. A table must have at least two columns, and the first column
must be an unsigned integer column called ``row_id``. This first column contains
the key used by Berkeley DB to store and retrieve rows. The values stored in this column
correspond to the zero-based row index, and they are automatically set by wormtable
when a new row is appended to a table.

The schema is defined using an XML format, which describes the names, types, sizes and
relative positions of the columns in a table.
Suppose we had the following wormtable schema, describing the first three columns of a
VCF file.

.. code-block:: xml

    <?xml version="1.0" ?>
    <schema version="0.1">
      <columns>
        <column name="row_id" element_size="5" element_type="uint" num_elements="1" description=""/>
        <column name="CHROM" element_size="1" element_type="char" num_elements="var(1)" description=""/>
        <column name="POS" element_size="5" element_type="uint" num_elements="1" description=""/>
      </columns>
    </schema>

In this schema, we have the mandatory ``row_id`` unsigned integer column, a ``CHROM`` column
which holds variable length character data, and a ``POS`` column, which also stores
five byte unsigned integers. When they are stored on disc, rows from this schema look
something like this:

.. image::  ../images/row-format.png
   :align: center
   :alt: The low-level row storage format
   :width: 15cm

Rows are divided into two regions: the **fixed** region and the **variable** region.
The fixed region occupies the first part of the row, and each column occupies
a fixed number of bytes within the fixed region. For columns with a fixed number
of elements, the number of bytes they occupy in the
fixed region is ``num_elements * element_size``; this is where the data for this
column is stored. Columns with a *variable* number of elements do not
store their data within the fixed region; instead, they store the **address**
and **number** of elements that are stored in this particular row. Addresses
then point to the variable region, which is filled sequentially as values are
assigned to columns within the row.

In the example above, ``row_id`` and ``POS`` are both unsigned integers with
an ``element_size`` of 5 and ``num_elements`` equal to 1, and so they both
occupy exactly 5 bytes within the fixed region (and never use the variable
region). The ``CHROM`` column on the other hand has an ``element_size`` of
1 byte (as each element is a single character), and has ``num_elements``
equal to ``var(1)``. This means that it is a variable length column
in which one byte is reserved to store the number of elements
stored in the variable region, starting at the address stored in the
fixed region.

Therefore, ``var(1)`` columns are assigned three bytes in the fixed
region; the first two hold the address where the elements for this column
start, and the third byte holds the number of elements stored. This
format defines the fundamental limits of wormtable's row format: since
we have two bytes to describe addresses, rows are a maximum of 64K long.
Since we have one byte to hold the number of elements in a
``var(1)`` column, we store a maximum of 255 elements within
a variable length column. However, ``var(2)`` columns are assigned
four bytes in the fixed region and can therefore
hold a maximum of 65535 elements.

**************
Column storage
**************

Values are stored in columns in a portable binary format. This binary format is
very close to the native representation and can be converted into native
types with very little overhead. The packed representation differs between
the element types, but there are two overriding requirements that apply to all
columns:

1) Missing values must be equal to 0 in the packed format;

2) Packed values must sort in the same order as the unpacked values.

The second requirement is particularly important, as this ensures that
indexes can be constructed by Berkeley DB without requiring a custom
ordering function.

