========
Tutorial
========

------------
Introduction
------------
Wormtable is a method for storing and interacting with large scale tabular 
datasets. It provides those familiar with python the necessary tools to 
efficiently store, process and search datasets of essentially unlimited size. It 
is designed for tablular data with many rows each with a fixed number of 
columns. Wormtable can generate indexes of the information in these rows which 
allows the user to quickly access the information. Since the indexes are stored 
on the disk they can be used repeatedly.

In this tutorial we will be taking you through the steps to convert a file to a 
wormtable, index columns and perform some basic operations on the data by using 
a few examples.

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

In the following examples we will be working with a small ~15,000 line sample 
VCF available from BLAH `sample.vcf.gz <http://sample.vcf.gz>`_.

Throughout this tutorial, code lines beginning ``$`` imply a bash shell and 
``>>>`` imply a python shell.

------------
Installation
------------
Before you get started make sure you have installed Berkeley DB and then 
Wormtable. For more details see the `installation instructions 
<https://pypi.python.org/pypi/wormtable>`_.

---------------------------------
Convert a VCF to Wormtable format
---------------------------------
To convert a standard VCF file to Wormtable format use the provided utility 
"vcf2wt". Like other utilities provided with Wormtable the description of 
command line options and arguments can be displayed by adding the "--help" flag 
after the utility::

	$ vcf2wt --help

Building a wormtable from a vcf file is easy::

	$ vcf2wt sample.vcf sample_wt

In this command the VCF file (sample.vcf) converted into a wormtable stored in 
the directory sample_wt. If the directory already exists you will have to use 
the "--force" (or -f) argument to tell vcf2wt to overwrite the old wormtable::

	$ vcf2wt -f sample.vcf sample_wt

Setting the cache size
----------------------
You can greatly increase the performance of Wormtable by tweaking the cache size 
parameter. The cache size determines how much of the table is held in memory. In 
general the more RAM you give the process the better it will perform. As as a 
rule of thumb try to give it half the available RAM. Later in this tutorial we 
will return to the issue of cache size as it can affect a number of performance 
components. To alter the cache size while making your wormtable use the 
--cache-size (-c) option ::

	$ vcf2wt -f -c 4G sample.vcf sample_wt

-----------------
Building an index
-----------------
At this point the vcf has been converted into a wormtable but in order to work 
with it, it is necessary to 'index' the columns that you are interested in.
Indexes provide a way to quickly and efficiently access information 
from the wormtable based on the values in a column. 

In the following example, we'll demonstrate how it is possible to access the 
DNA sequence of the reference genome (which is stored in the "*REF*" column) 
for different positions in the genome by creating an index on genomic position.
Adding an index for a column can be accomlished with the wtadmin utility. In
our example, to index the position column called "*POS*" we use::

	$ wtadmin add sample_wt POS

Here, sample_wt is the "home directory" which contains our wormtable and POS 
is the name of the column to be indexed. This utility also allows us to remove 
indexes (wtadmin rm) or list the columns already indexed (wtadmin ls).
If you want to list the columns that are available to index use ::

 	$ wtadmin show sample_wt

Similar to the cache size when building our wormtable, we can set the cache size 
when building an index. A large cache size can reduce the time it takes to 
build an index ::

	$ wtadmin add --index-cache-size 4G sample_wt POS 

--------------
Using an index
--------------
Now that we have built our wormtable and indexed on POS we can use the python 
wormtable module (within an interactive python shell) to interact with our new 
wormtable and index::

	>>> import wormtable
	>>> # Open the wormtable using the open_table function
	>>> table = wormtable.open_table('sample_wt')
	>>> # Open the index that was built using wtadmin (see above)
	>>> position_index = table.open_index('POS')

Note that if you have not already added the index using wtadmin add you will not 
be able to open the index in python. Also, worth noting is that, like cache sizes,
when building tables or adding indexes we can assign memory to both the table 
and index when we open them by including the cache size as a second argument in 
opentable() or open_index(). For more details see 
`Performance tuning <http://jeromekelleher.github.io/wormtable/performance.html>`_. 

The Wormtable python module offers a number of methods to interact with an index::

	>>> # Print the minimum and maximum value of an index
	>>> position_index.get_min()
	>>> position_index.get_max()
	>>> # Use keys() to iterate through sorted value in the index
	>>> all_keys = [i for i in position_index.keys()]

--------------
Using a cursor
--------------
Another convenient feature provided by the wormtable python module is the 
"cursor", which allows us to retrieve information from any column of our 
wormtable for ranges of values from our indexed column. In our case, we will 
create a cursor to return the REF column for specific genomic positions ::

	>>> c = table.cursor(["REF"], position_index)

Note that since we can retrieve information from multiple columns, the names 
of the columns we want to retrieve are passed to the cursor as a list. 

We can set the minimum and maximum values for which the cursor will return 
columns::

	>>> c.set_min(8000000)
	>>> c.set_max(8000500)

and then iterate through positions in this range (8000000-8000500), returning 
the *REF* column for each row of the table::

	>>> for p in c:
	>>> 	print p[0] 
	
Note that by default the cursor will return a tuple and we just
print the the first element. 

[Dan: Add something here about inclusive/excludive starts / ends]
[Rob: I am not sure what you mean - like 1:100 includes 1 but not 100?]
[Dan: Jerome will need to clear this up but I think that if you do
set_max(100), then 100 will not be included. There is also some complication
about how this then functions with binned indexes. Perhaps we should just
refer to other documentation at this point.]

-------------------------
Creating compound indexes
-------------------------
With multiple chromsomes, the example above will fail because the *POS* 
column does not necessarily identify a single position. As a result our cursor 
will iterate over positions matching the range specified from multiple 
chromosomes. To deal with this we can can make compound indexes. Compound 
indexes allow the user identify all combinations of multiple columns from the 
wormtable. For example we can make a compound index of chromosome (*CHROM*) and 
position (*POS*) to retrieve unique genomic positions. To add a compound column 
we can again use the wtadmin utility ::

	$ wtadmin add sample_wt CHROM+POS

Note that in this case the names of multiple columns are joined using "+" which 
indicates to wtadmin to make a compound index. It is important to realise that 
the order that the columns are listed matters (CHROM+POS does not equal 
POS+CHROM). With this new compound column we can specify a region of the genome 
(chromosome 1, positions 8000000 to 8000500) unambiguously and iterate 
through rows in this region, printing CHROM, POS and REF for each::

	>>> import wormtable
	>>> t = wormtable.open_table('sample_wt')
	>>> chrompos_index = table.open_index('CHROM+POS')
	>>> c = t.cursor(['CHROM','POS','REF'], chrompos_index)
	>>> c.set_min('1',8000000)
	>>> c.set_max('1',8000500)
	>>> for p in c:
	>>> 	print p
	
[Dan: At some point we need to discuss the naming of info and genotype columns
with underscores e.g. INFO_DP etc.]

-----------------
Using the counter
-----------------
Another useful feature of Wormtable is the ability to count the number of items 
matching unique keys in an index. The counter is a dictionary-like 
object where the keys are index values which refer to the number of times that 
index occurs. For example, we can quickly and efficiently calculate the 
fraction of reference sites that are G or C (the GC content) by first creating
an index on the *REF* column::

	$ wtadmin add sample_wt REF

Then in python ::

	>>> ref_index = t.open_index('REF')
	>>> ref_counts = ref_index.counter()
	>>> gc = ref_counts['G'] + ref_counts['C']
	>>> tot = gc + ref_counts['T'] + ref_counts['A']
	>>> float(gc) / float(tot)

--------------------
Using binned indexes
--------------------
Some columns in a VCF contain floats and can therefore have a huge number of 
distinct values. In these cases it may be useful to condense similar values into 
'binned' indexes. For example, in a VCF the column which records the quality of 
row (QUAL column) is a float which may range from 0 to 10,000 (or more) and you 
may not want to discern between sites with quality of 50.1 from sites with 
quality of 50.2. Using wtadmin you can index a column binning indexes into equal 
sized bins like this ::

	$ wtadmin add sample_wt QUAL[5]

This will make a new index on QUAL where all the QUAL values are grouped into 
bins of width 5. We can then use this binned index interact with our wormtable 
and print the number of rows matching QUAL scores in bins between 0 and 100 using 
the counted function ::

	>>> qual_5_index = t.open_index('QUAL[5]')
	>>> qual_5_counter = qual_5_index.counter()
	>>> for quality in range(0,101,5):
	>>> 	print q, qual_5_counter[q]



--------
Examples
--------
Along with the main program we have included a number of example scripts which 
will help you get started with Wormtable. These scripts highlight more of 
Wormtable's features and may be easily modified to suit your own purposes. If 
you want to read up on how these examples work and write your own scripts for 
Wormtable, full documentation can be found 
`here <http://jeromekelleher.github.io/wormtable/>`_. 

Count the distinct index values - *count-distinct.py*
-----------------------------------------------------

This script will take the name of any wormtable home directory and column which 
has been indexed and print each distinct value in that column and the number of 
times it occurs ::

	$ python count-distinct.py sample_wt REF

Transition-Transversion ratio - *ts-tv.py*
------------------------------------------
This uses a compound index of the reference nucleotide *REF* and the alternate 
nucleotide *ALT* to count the number of transitions (changes A<->G or C<->T) and 
transversions (A/G<->C/T). Using the counter feature this task can be very fast 
with Wormtable ::

	$ wtadmin add sample_wt REF+ALT # in case index does not already exist.
	$ python ts-tv.py sample_wt

High Quality SNPs - *hq-snps.py*
--------------------------------
In this example we provide a script that will return all the sites in your VCF 
that have a quality score over a particular minimum threshold. This script uses 
a QUAL index where QUAL scores have been grouped into bins of width 1 (QUAL[1]) 
::

	$ wtadmin add sample_wt QUAL[1] # in case index does not already exist.
	$ python hq-snps.py -q 30 sample_wt

Sliding window analysis of Genetic Diversity - *sliding-window.py*
-------------------------------------------------------------------
This script demonstrates how we can use the cursor feature of Wormtable to move 
through a file in windows and perform calculations on those windows. In this 
case we calculate the amount of genetic diversity that is present in each window 
using the alternate allele frequency (*AF* column) or by calculating the 
alternate allele frequency using the genotype calls in the sample columns.

