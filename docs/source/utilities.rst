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
    print("example.wt as ", len(t), "rows")
    t.close()

The :class:`Table` class also supports the 
`context manager <http://www.python.org/dev/peps/pep-0343/>`_
protocol, so we can automatically close a table that has 
been opened::

    import wormtable as wt
    with open_table("example.wt") as t:
        print("example.wt as ", len(t), "rows")


---------
Reference
---------

.. autofunction:: open_table

.. autoclass:: Table

.. autoclass:: Index




