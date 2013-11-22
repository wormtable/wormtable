.. _tutorial-index:

========
Tutorial
========

:Release: |version|
:Date: |today|

.. py:currentmodule:: wormtable 

Wormtable is a method for storing and interacting with large scale tabular 
datasets. It provides those familiar with python the necessary tools to 
efficiently store, process and search datasets of essentially unlimited size. It 
is designed for tabular data with many rows each with a fixed number of 
columns. Wormtable can generate indexes of the information in these rows which 
allows the user to quickly access the information. Since the indexes are stored 
on the disk they can be used repeatedly.

In this tutorial we will be taking you through the steps to convert a file to a 
wormtable, index columns and perform some basic operations on the data by using 
a few examples.

Before you get started make sure you have installed Berkeley DB and then 
Wormtable. For more details see the `installation instructions 
<https://pypi.python.org/pypi/wormtable>`_.

Throughout this tutorial, code lines beginning ``$`` imply a bash shell and 
``>>>`` imply a python shell.

--------------
The VCF format 
--------------
Throughout this tutorial we will be using a *Variant Call 
Format* (VCF) table.  This is a common format for storing DNA sequence and 
polymorphism data from high throughput genome sequencing projects. In this 
format rows are individual positions of a genome and are identified by the 
chromosome they occur on and the position on that chromosome. A variety of other 
pieces of information are stored as metadata in the proceeding columns. We will 
explain the relevant columns as they arise. For more information please consult 
the full specifications of a VCF file on the `1000 genomes website  
<http://www.1000genomes.org/wiki/analysis/vcf4.0/>`_. 

In the following examples we will be working with the following example VCF file with 
only 5 genomic positions taken from the `1000 genomes website  
<http://www.1000genomes.org/wiki/analysis/vcf4.0/>`_. ::

    ##fileformat=VCFv4.0
    ##fileDate=20090805
    ##source=myImputationProgramV3.1
    ##reference=1000GenomesPilot-NCBI36
    ##phasing=partial
    ##INFO=<ID=NS,Number=1,Type=Integer,Description="Number of Samples With Data">
    ##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
    ##INFO=<ID=AF,Number=.,Type=Float,Description="Allele Frequency">
    ##INFO=<ID=AA,Number=1,Type=String,Description="Ancestral Allele">
    ##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership, build 129">
    ##INFO=<ID=H2,Number=0,Type=Flag,Description="HapMap2 membership">
    ##FILTER=<ID=q10,Description="Quality below 10">
    ##FILTER=<ID=s50,Description="Less than 50% of samples have data">
    ##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
    ##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
    ##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
    ##FORMAT=<ID=HQ,Number=2,Type=Integer,Description="Haplotype Quality">
    #CHROM POS     ID        REF ALT    QUAL FILTER INFO                              FORMAT      NA00001        NA00002        NA00003
    20     14370   rs6054257 G      A       29   PASS   NS=3;DP=14;AF=0.5;DB;H2           GT:GQ:DP:HQ 0|0:48:1:51,51 1|0:48:8:51,51 1/1:43:5:.,.
    20     17330   .         T      A       3    q10    NS=3;DP=11;AF=0.017               GT:GQ:DP:HQ 0|0:49:3:58,50 0|1:3:5:65,3   0/0:41:3
    20     1110696 rs6040355 A      G,T     67   PASS   NS=2;DP=10;AF=0.333,0.667;AA=T;DB GT:GQ:DP:HQ 1|2:21:6:23,27 2|1:2:0:18,2   2/2:35:4
    20     1230237 .         T      .       47   PASS   NS=3;DP=13;AA=T                   GT:GQ:DP:HQ 0|0:54:7:56,60 0|0:48:4:51,51 0/0:61:2
    20     1234567 microsat1 GTCT   G,GTACT 50   PASS   NS=3;DP=9;AA=G                    GT:GQ:DP    0/1:35:4       0/2:17:2       1/1:40:3


---------------------------------
Convert a VCF to Wormtable format
---------------------------------
To convert a standard VCF file to Wormtable format use the provided utility 
``vcf2wt``. Like other utilities provided with Wormtable the description of 
command line options and arguments can be displayed using "--help" flag ::

    $ vcf2wt --help

Building a wormtable from a vcf file is easy::

    $ vcf2wt sample.vcf sample.wt

In this command the VCF file (sample.vcf) converted into a wormtable stored in 
the directory sample.wt. If the directory already exists you will have to use 
the "--force" (or -f) argument to tell vcf2wt to overwrite the old wormtable::

    $ vcf2wt -f sample.vcf sample.wt


.. warning:: Wormtable does not currently support very long strings, so it 
   may be necessary to truncate the ``ALT`` and ``REF`` columns when converting 
   a VCF. Use the ``--truncate`` option to ``vcf2wt`` to do this.
   
--------------
Using a cursor
--------------
Now that we have built our wormtable we can use the Python :mod:`wormtable` module 
(within a python shell) to interact with it::

    >>> import wormtable
    >>> table = wormtable.open_table('sample.wt') # open the wormtable

A convenient feature of Wormtable is the :meth:`Table.cursor` method 
which allows us to retrieve information from any column in the table. In 
our case, we use a cursor to return the genome position column "CHROM" 
and "POS". The cursor allows us to walk through the wormtable row by row ::

    >>> for row in table.cursor(['CHROM', 'POS']):
    ...     print(row)
    ... 
    (b'20', 14370)
    (b'20', 17330)
    (b'20', 1110696)
    (b'20', 1230237)
    (b'20', 1234567)

Note that since we can retrieve information from multiple columns, the names 
of the columns we want to retrieve are passed to the cursor as a list. 

.. warning:: All character data in wormtable is returned as *bytes*
   values. For Python 3 users, this means they are not the same as strings, 
   but must be *decoded*. For Python 2 users, there is no distinction 
   between bytes and strings.

-----------------
Building an index
-----------------
To fully exploit a wormtable, it is necessary to *index* the columns 
that you are interested in. Indexes provide a way to quickly and efficiently 
access information from the wormtable based on the values in the indexed column. 

In the following example, we'll demonstrate how it is possible to access the 
DNA sequence of the reference genome (which is stored in the "*REF*" column) 
for any position in the genome by creating an index on genomic position. Adding 
an index for a column can be accomplished with the ``wtadmin`` utility. In this 
example, to index the position column called "*POS*" we use::

    $ wtadmin add sample.wt POS

Here, ``sample.wt`` is the "home directory" which contains our wormtable and POS 
is the name of the column to be indexed. This utility also allows us to remove 
indexes (``wtadmin rm``) or list the columns already indexed (``wtadmin ls``).
If you want to list the columns that are available to index use ::

    $ wtadmin show sample.wt
    ==============================================================
           name         type     size   n        |   description
    ==============================================================
       0   row_id       uint        5   1        |   Primary key column
       1   CHROM        char        1   var(1)   |   chromosome: an identifier from the reference genome or an angle-bracketed ID String ("<ID>") pointing to a contig in the assembly file
       2   POS          uint        5   1        |   position: The reference position, with the 1st base having position 1
       3   ID           char        1   var(1)   |   semi-colon separated list of unique identifiers where available
       4   REF          char        1   var(1)   |   reference base(s): Each base must be one of A,C,G,T,N (case insensitive)
       5   ALT          char        1   var(1)   |   comma separated list of alternate non-reference allelescalled on at least one of the samples
       6   QUAL         float       4   1        |   phred-scaled quality score for the assertion made in ALT. i.e. -10log_10 prob(call in ALT is wrong).
       7   FILTER       char        1   var(1)   |   PASS if this position has passed all filters, i.e. a call is made at this position. Otherwise, if the site has not passed all filters, a semicolon-separated list of codes for filters that fail. 
       8   INFO.NS      int         4   1        |   Number of Samples With Data
       9   INFO.DP      int         4   1        |   Total Depth
      10   INFO.AF      float       4   var(1)   |   Allele Frequency
      11   INFO.AA      char        1   var(1)   |   Ancestral Allele
      12   INFO.DB      uint        1   1        |   dbSNP membership, build 129
      13   INFO.H2      uint        1   1        |   HapMap2 membership
      14   NA00001.GT   char        1   var(1)   |   Genotype
      15   NA00001.GQ   int         4   1        |   Genotype Quality
      16   NA00001.DP   int         4   1        |   Read Depth
      17   NA00001.HQ   int         4   2        |   Haplotype Quality
      18   NA00002.GT   char        1   var(1)   |   Genotype
      19   NA00002.GQ   int         4   1        |   Genotype Quality
      20   NA00002.DP   int         4   1        |   Read Depth
      21   NA00002.HQ   int         4   2        |   Haplotype Quality
      22   NA00003.GT   char        1   var(1)   |   Genotype
      23   NA00003.GQ   int         4   1        |   Genotype Quality
      24   NA00003.DP   int         4   1        |   Read Depth
      25   NA00003.HQ   int         4   2        |   Haplotype Quality

Note that fields within the INFO column and the columns corresponding for 
individual samples have been represented as separate columns and named as 
[COLUMN].[FIELD]. This allows the user to create indexes on individual fields from these
compound columns.

When building an index over a large table it can be useful to set the 
cache size to improve performance. A large cache size allows more of the 
index to fit into memory, therefore making the process more efficient.  ::

    $ wtadmin add --cache-size=4G sample.wt POS 

--------------
Using an index
--------------
Now that we have built our wormtable and indexed on POS we can retrieve information 
from any position in the genome ::

    >>> import wormtable
    >>> table = wormtable.open_table('sample.wt') # open the wormtable
    >>> position_index = table.open_index('POS')  # open the index on POS

Note that if you have not already added the index using ``wtadmin add`` you will not 
be able to open the index in python. Also, worth noting is that, like cache sizes
when building tables or adding indexes, we can assign memory to both the table 
and index when we open them by including the ``db_cache_size`` as a second argument in 
:func:`open_table` or :meth:`Table.open_index`. For more details see 
the sections on :ref:`performance tuning <performance-index>`.
The wormtable module offers a number of methods to interact with an 
:class:`Index` ::

    >>> # Print the minimum and maximum keys in an index
    >>> position_index.min_key()
    14370
    >>> position_index.max_key()
    1234567
    >>> # Use keys() to iterate through sorted value in the index
    >>> for k in position_index.keys():
    ...     print(k)
    ... 
    14370
    17330
    1110696
    1230237
    1234567

The :class:`Index` class also provides a :meth:`Index.cursor` method 
to iterate over rows in a table. In the case of an index, however,
we visit the rows in the order defined by the index and can access 
rows based on the index keys using the *start* and *stop* parameters.
For example, to retrieve the reference nucleotides we can use a cursor to return the REF 
column for genomic positions in the range 1--1150000 using::

    >>> for p in position_index.cursor(["REF"], start=1, stop=1150000): 
    ...     print(p[0]) 
    ... 
    b'G'
    b'T'
    b'A'

Note that the cursor always returns a tuple and we just 
print the first element here. It is also worth noting that like other 
ranges in Python, the maximum value is not included. For example, 
1 to 100 would return 1 to 99 and not include 100.

-------------------------
Creating compound indexes
-------------------------
With multiple chromosomes, the example above could give multiple values for each position 
because the *POS* column is not normally a unique identifier of genomic position and our 
cursor will iterate over positions matching the range specified from multiple 
chromosomes. To deal with this we can can make compound indexes. Compound 
indexes allow the user identify all combinations of multiple columns from the 
wormtable. For example, we can make a compound index of chromosome (*CHROM*) and 
position (*POS*) to retrieve unique genomic positions. To add a compound column 
we can again use the ``wtadmin`` utility ::

    $ wtadmin add sample.wt CHROM+POS

The names of multiple columns in a compound index are joined using "+" which 
indicates to ``wtadmin`` to make a compound index. It is important to realise that 
the order that the columns are listed matters (CHROM+POS does not equal 
POS+CHROM). With this new compound index we can specify a region of the genome 
(chromosome 20, positions 1 to 1150000) unambiguously and iterate 
through rows in this region, printing CHROM, POS and REF for each::

    >>> import wormtable
    >>> table = wormtable.open_table('sample.wt')
    >>> chrompos_index = table.open_index('CHROM+POS')
    >>> cols = ["CHROM", "POS", "REF"]
    >>> for c, p, r in chrompos_index.cursor(cols, start=("20", 1), stop=("20", 1150000)):
    ...     print(c, p, r)
    ... 
    b'20' 14370 b'G'
    b'20' 17330 b'T'
    b'20' 1110696 b'A'

Since we need to specify values for several columns, the *start* and *stop* arguments 
are tuples.

---------------
Using a counter
---------------
Another useful feature of Wormtable is the ability to count the number of items 
matching unique keys in an index. A :class:`Counter` is a dictionary-like 
object where the keys are index values which refer to the number of times that 
key occurs in the table. For example, we can quickly and efficiently calculate the 
fraction of reference sites that are G or C (the GC content) by first creating
an index on the *REF* column::

    $ wtadmin add sample.wt REF

Then in python: ::

    >>> import wormtable
    >>> table = wormtable.open_table('sample.wt')
    >>> ref_index = table.open_index('REF')
    >>> ref_counts = ref_index.counter()
    >>> gc = ref_counts[b'G'] + ref_counts[b'C']
    >>> tot = gc + ref_counts[b'T'] + ref_counts[b'A']
    >>> gc / tot
    0.25

--------------------
Using binned indexes
--------------------
Some columns in a VCF contain floats and can therefore have a huge number of 
distinct values. In these cases it is useful to condense similar values into 
'binned' indexes. For example, in a VCF the column which records the quality of 
a row (QUAL column) is a float which may range from 0 to 10,000 (or more). For the 
purposes of filtering on this column (i.e. creating an index) it may not be 
necessary to discern between sites with quality of 50.1 from sites with quality 
of 50.2. Using ``wtadmin`` you can index a column binning indexes into equal sized 
bins of size ``n`` like this ::

    $ wtadmin add sample.wt QUAL[n]

where n is an integer or float. This will make a new index on QUAL where all the QUAL 
values are grouped into bins of size n. We can then use this binned index 
to interact with our wormtable and print the number of rows matching QUAL scores 
in bins between 0 and 70 using the :meth:`Index.counter` function.
For example, to create an index with bin size 5, we use:: 

    $ wtadmin add sample.wt QUAL[5]

Then, we can quickly count the number of rows falling into each bin::

    >>> qual_5_index = table.open_index('QUAL[5]')
    >>> qual_5_counter = qual_5_index.counter()
    >>> for q in range(0, 70, 5):
    ...     print(q, "\t", qual_5_counter[q])
    ... 
    0    1
    5    0
    10   0
    15   0
    20   0
    25   1
    30   0
    35   0
    40   0
    45   1
    50   1
    55   0
    60   0
    65   1


--------
Examples
--------
Along with the main program we have included a number of example scripts which 
will help you get started with using Wormtable. The full scripts are available should 
you want to use or modify the example scripts for your own purposes.

*******************
Counting index keys
*******************

In this example we use an index counter to get an iterator over the keys 
and their counts in an index. We use the 
`context manager <http://www.python.org/dev/peps/pep-0343/>`_ protocol
(the ``with`` statement) to ensure that the table and index 
are closed when we finish.

.. code-block:: python 
    
    import wormtable as wt
    
    def count_distinct(homedir, index):
        with wt.open_table(homedir) as t, t.open_index(index) as i: 
            for k,v in i.counter().items():
                yield k, v

Using this function we can easily print out all of the values in the 
REF column and their counts::

    >>> for k, v in count_distinct("sample.wt", "REF"): 
    ...     print(k, "\t", v)
    ... 
    b'A'     1
    b'G'     1
    b'GTC'   1
    b'T'     2

This functionality is also provided by the ``wtadmin hist`` command::

    $ wtadmin hist sample.wt REF
    # n REF
    1    A
    1    G
    1    GTC
    2    T


*****************************
Transition-Transversion ratio
*****************************

This example uses a compound index of the reference nucleotide (*REF*) and the alternate 
nucleotide (*ALT*) to count the number of transitions (changes A <-> G or C <-> T) and 
transversions (A or G <-> C or T). Using the counter feature this task can be very fast 
with Wormtable. First we use Python's :mod:`itertools` to generate a list of all possible 
single bases changes (ie all pairs of A,C,G and T). We then count the number of
instances of each change in our data ::

    import wormtable
    from itertools import permutations
    def count_Ts_Tv(homedir):
        """ 
        Count number of of transitions and transversions using an index on REF+ALT
        """
        subs = [p for p in permutations([b'A',b'C',b'G',b'T'], 2)]
        bases = {b'A':'purine', b'G':'purine', b'C':'pyrimidine', b'T':'pyrimidine'}
        t = wormtable.open_table(homedir)
        i = t.open_index("REF+ALT")
        Ts, Tv = 0, 0
        c = i.counter()
        for s in subs:
            if bases[s[0]] == bases[s[1]]: 
                Ts += c[s] 
            else: 
                Tv += c[s] 
        i.close()
        t.close()
        return Ts, Tv

we can then use this function to very quickly count the number of 
transitions and transversions: ::

    >>> count_Ts_Tv('sample.wt')
    (1, 1)

*****************
High Quality SNPs 
*****************

In this example we wish to examine the sites in a VCF 
that have a quality score over a particular minimum threshold. The function 
uses a QUAL index where QUAL scores have been grouped into bins of 
width 1 (QUAL[1]), and returns an iterator over all of the rows
that fulfil the given quality requirements. ::

    import wormtable as wt
    def hq_snps(homedir, minq, cols):
        with wt.open_table(homedir) as t, t.open_index("QUAL[1]") as i:
            for row in i.cursor(cols, start=minq):
                yield row 

First we must create the required index::

    $ wtadmin add sample.wt QUAL[1] 

We can then use this function in to iterate over the rows of interest: ::

    >>> for row in hq_snps('sample.wt', 30, ['CHROM', 'POS', 'REF', 'ALT', 'QUAL']):
    ...     print(row)
    ... 
    (b'20', 1230237, b'T', b'', 47.0)
    (b'20', 1234567, b'GTC', b'G,GTCT', 50.0)
    (b'20', 1110696, b'A', b'G,T', 67.0)

-------------
VCF-Utilities
-------------

We have also provided three utilities (in the directory 
examples/vcf-utils) which will allow a user to use wormtable with VCF format 
files immediately. These scripts demonstrate the efficiency of using Wormtable 
with VCF files and are described briefly below.

*************
snp-filter.py
*************

This script runs through a VCF file (using a CHROM+POS compound index) and allows 
the user to extract (a semicolon separated list of) specific VCF fields using an 
arbitrary set of filters on numeric or text columns. For example, to 
find variants with a QUAL score > 500, depth of coverage (stored as DP in the 
INFO column) > 20, a genotype in sample "S1" of "0/1" and print out CHROM and 
POS for variants in a wormtable stored in sample.wt, the user can 
use the following call ::

    snp-filter.py -f 'QUAL>500;INFO.DP>20;S1.GT==0/1' CHROM,POS sample.wt
    
The user can also optionally specify a particular region of the VCF using the
CHROM:START-END syntax and either exclude, include or find indels.

***************
sliding-mean.py
***************

This script takes a comma separated list of numeric columns and the home directory 
containing the wormtable and will then calculate the mean of these 
numeric columns within non-overlapping windows (using an optionally specified 
window size and list of chromosomes). The output is in tab separated column 
format allowing the results to be easily plotted. For example, to calculate the
mean of QUAL and depth of coverage (INFO.DP) in window sizes of 1Mb for 
chromosomes 1,2 and 3 from a wormtable stored in sample.wt, run ::

    sliding-mean.py QUAL,INFO.DP 20 -w 1000000 sample.wt

***************
hq-snps-bygt.py
***************

This script takes a sample name and a specific genotype code, then builds a
compound index on the sample genotype columns and quality score allowing the
user to find, for example, high quality heterozygotes for the first sample. For 
example, to very efficiently obtain high quality heterozygotes (QUAL>10000) from 
sample NA00001, run ::

    hq-snps-bygt.py -s NA00001 -g '0/1' -q 50 sample.wt 




