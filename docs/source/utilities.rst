.. _utilities-index:

======================
Command line utilities
======================

:Release: |version|
:Date: |today|

.. module:: wormtable 
    :platform: Unix
    :synopsis: Write-once read-many table for large datasets. 

Wormtable is distributed with two command line utility programs,
:ref:`vcf2wt-index` and :ref:`wtadmin-index`. These utilities provide a convenient 
means of creating wormtables from VCF data, and to administer 
existing wormtables.

.. _vcf2wt-index:

--------
vcf2wt
--------

.. program:: vcf2wt 

Converts data files written in the 
`Variant Call Format <http://vcftools.sourceforge.net/specs.html>`_
(VCF) to wormtable. See `vcf2wt --help` for detailed program options.

.. _wtadmin-index:

---------
wtadmin
---------

.. program:: wtadmin 

Administration tool for wormtable. Provides commands to add and remove 
indexes, list the indexes present, show the columns in the table
and to dump rows from the table stdout. See `wtadmin --help` for 
details and program options.

