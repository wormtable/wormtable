========
Tutorial
========

------------
Introduction
------------
Wormtable is a method for storing and interacting with large scale tabular 
datasets. It provides those familiar with python the necessary tools to efficiently 
store, process and search datasets of essentially unlimited size. It is designed
for tablular data with many rows each with a fixed number of columns. Wormtable 
can generate indexes of the information in these rows which allows the user to 
quickly access the information. Since the indexes are stored on the disk they 
can be used repeatedly.

In this tutorial we will be taking you through the steps to convert a file to a 
wormtable, index columns and perform some basic operations on the data by using
examples.

**The VCF format:** Throughout this tutorial we will be using a *Variant Call 
Format* (VCF) table.  This is a common format for storing DNA sequence and 
polymorphism data from high throughput genome sequencing projects. In this 
format rows are individual positions of a genome and are identified by the 
chromosome they occur on and the position on that chromosome. A variety of 
other pieces of information are stored as metadata in the proceeding columns. 
We will explain the relevant columns as they arise. For more information please 
consult the full specifications of a VCF file on the 
`1000 genomes website  <http://www.1000genomes.org/wiki/analysis/vcf4.0/>`_. 

In the following examples we will be working with a small ~15,000 line sample 
VCF available from BLAH `sample.vcf.gz <http://sample.vcf.gz>`_.

------------
Installation
------------

Before you get started make sure you have installed Berkeley DB and then 
Wormtable. For more details see the `installation instructions 
<https://pypi.python.org/pypi/wormtable>`_.

---------------------------------
Convert a VCF to Wormtable format
---------------------------------

To convert a standard VCF file to WormTable format use the provided utility 
"vcf2wt". Like other utilities provided with Wormtable the description of 
command line options and arguments can be displayed by adding the "--help" flag 
after the utility ::

	$ vcf2wt --help

Building a wormtable from a vcf file is easy using this command ::

	$ vcf2wt sample.vcf sample_wt

In this command the VCF file called sample.vcf is read into a wormtable in the 
folder sample_wt. If the folder already exists you will have to use the 
"--force" (or -f) argument to tell vcf2wt to overwrite the old wormtable::

	$ vcf2wt -f sample.vcf sample_wt

<<<<<<< HEAD
Setting the cache size
----------------------
You can greatly increase the performance of Wormtable by tweaking the cache size 
parameter. The cache size determines how much of the table is held in memory. In 
general the more RAM you give the process the better it will perform. As as a 
rule of thumb try to give it half the available RAM. Later in this tutorial we 
will return to the issue of cache size as it can affect a number of performance 
components. To alter the cache size while making your wormtable use the 
--cache-size (-c) option ::

	$ vcf2wt -f -c 4G sample.vcf sample_wt/
=======
For more details on the usage of vcf2wt see section BLAH.
Something about the cache [Dan: save this till later?]

>>>>>>> a71ceaaa4afd9ee6f4b113c6301639498e3b4c97

---------------------------------
Building an index
---------------------------------

<<<<<<< HEAD
At this point your vcf has been converted into a wormtable but in order to work 
with it you need to choose what columns you're interested in and 'index' those 
columns. The index provides a way to quickly and efficiently access information 
from the wormtable based on the values in the indexed column. For example, if 
we are interested in knowing the DNA sequence of the reference genome (which is 
stored in the "*REF*" column) we can simply ask for the value of the *REF* 
column across a number of rows in the wormtable that correspond to a piece of 
the reference genome.

To accomplish this we first need to index a few columns. Indexing columns, along 
with a number of other tools for administrating your wormtable, are done with 
the wtadmin utility. Amongst other features, wtadmin allows us to add indexes 
(wtadmin add), remove indexes (wtadmin rm) or list the columns already indexed 
(wtadmin ls). If we want to access the rows of our table according to their 
position in the genome we need to index the position column called "*POS*"::

	$ wtadmin add sample_wt/ POS

Here the "sample_wt" is the homedirectory which contains our wormtable and POS 
is the name of the column for which we built an index. 

Similar to the cache size when building our wormtable, we can set the cache size 
when building an index. A large cache size can reduce the time it takes to 
build an index ::

	$ wtadmin add --index-cache-size 4G sample_wt/ REF 

If you want to list the columns that are available to index use ::
=======
At this point the vcf has been converted into a wormtable but in order to work 
with it, it is necessary to 'index' the columns that you are interested in. These
indexes provide a way to quickly and efficiently access information 
from the wormtable based on the values in the indexed column. 

In the following example, we'll demonstrate how it is possible to access the 
DNA sequence of the reference genome (which is stored in the "*REF*" column) 
for different positions in the genome by creating an index on genomic position.
Adding an index for a column can be accomlished with the wtadmin utility. In
our examples, to index the position column called "*POS*" we use::

	$ wtadmin add sample_wt POS

This utility also allows us to remove indexes (wtadmin rm) or list the columns 
already indexed (wtadmin ls).

Here the "sample_wt" is the "home directory" which contains our wormtable and POS 
is the name of the column to be indexed. If you want to list the 
columns that are available to index use ::
>>>>>>> a71ceaaa4afd9ee6f4b113c6301639498e3b4c97

 	$ wtadmin show sample_wt

Now that we have our wormtable built and POS indexed we can use Python to 
interact with our new wormtable and index ::

	$ python
	>>> import wormtable

	>>> # Open the wormtable using the open_table function
	>>> t = wormtable.open_table('sample_wt')

	>>> # Open the index that was built using wtadmin (see above)
	>>> position_index = t.open_index('POS')

Note that if you have not already added the index using wtadmin add you will not 
be able to open the index in python. Also, worth noting is that like cache sizes 
when building tables or adding indexes we can assign memory to both the table 
and index when we open them by including the cache size as a second argument in 
opentable() or open_index() - for more details read `Performance tuning <http://jeromekelleher.github.io/wormtable/performance.html>` _.) 

<<<<<<< HEAD
The Wormtable module offers a number of methods to interact with the data in your wormtable ::
=======
Note that if you have not already added the index using wtadmin add you won't be 
able to open the index in python. 

The Wormtable module offers a number of methods to interact with the data in 
your wormtable ::
>>>>>>> a71ceaaa4afd9ee6f4b113c6301639498e3b4c97

	>>> # Print the minimum and maximum value of an index
	>>> position_index.get_min()
	>>> position_index.get_max()
	
	>>> # Use keys() to iterate through sorted value in the index
	>>> all_keys = [i for i in position_index.keys()]

Another convenient feature is the "cursor", which allows us to retrieve 
information from any column of our wormtable over ranges of values from our 
indexed column. In our case, we will create a cursor to return the REF column 
indexed by genomic position ::

	>>> c = t.cursor(["REF"], position_index)

The names of the columns we want to retrieve are passed to the cursor as a list. 
We can set the minimum and maximum values for which the cursor will return 
columns ::

	>>> c.set_min(8000000)
	>>> c.set_max(8000500)

Now we can iterate through the *REF* columns from genomic positions with *POS* 
values between 8000000 and 8000500 ::

	>>> for p in c:
	>>> 	print p[0] # By default the cursor will return a tuple so take the first element returns a string 

However, you may have noticed this example isn't quite right. The *POS* column 
does not necessarily identify a single position in the genome because multiple 
chromosomes will have the same position. To deal with this we can can make 
compound indexes, another powerful feature of Wormtable. Compound indexes allow 
the user identify all combinations of multiple columns from the wormtable. For 
example we can make a compound index of chromosome (*CHROM*) and position 
(*POS*) to retrieve unique genomic positions. To add a compound column we can 
again use the wtadmin utility ::

	$ wtadmin add sample_wt CHROM+POS

Note that in this case the names of multiple columns are joined using "+" which 
indicates to wtadmin to make a compound index. It is important to realise that 
the order that the columns are listed matters. CHROM+POS does not equal 
POS+CHROM. With this new compound column we can specify a region of the genome 
unambiguously ::

	>>> import wormtable
	>>> t = wormtable.open_table('sample_wt')
	>>> chrompos_index = t.open_index('CHROM+POS')
	>>> c = t.cursor(["REF"],chrompos_index)
	>>> c.set_min('1',8000000)
	>>> c.set_max('1',8000500)
	>>> for p in c:
	>>> 	print p[0]

-----------------
Using the Counter
-----------------
Another useful feature of Wormtable is that the number of times a particular 
index value occurs is simple to retrieve. The counter is a dictionary-like 
object where the keys are index values which refer to the number of times that 
index occurs. For example, we can quickly and efficiently calculate the 
fraction of reference sites that are G or C (the GC content) ::

	>>> ref_index = t.open_index('REF')
	>>> ref_counts = ref_index.counter()
	>>> GC_content = float(ref_counts['G'] + ref_counts['C']) / (ref_counts['T'] + ref_counts['A'] + ref_counts['G'] + ref_counts['C'])

----------------------------------
Using binned indexes
----------------------------------
Some columns in a VCF contain floats and can therefore have a huge number of 
distinct values. In these cases it may be useful to condense similar values 
into 'binned' indexes. For example, in a VCF the column which records the 
quality of row (QUAL column) is a float which may range from 0 to 10,000 (or 
more) and you may not want to discern between sites with quality of 50.1 from 
sites with quality of 50.2. Using wtadmin you can index a column binning 
indexes into equal sized bins like this ::

	$ wtadmin add sample_wt/ QUAL[5]

This will make a new index on QUAL where all the QUAL values are grouped into 
bins of width 5. We can then use this binned index interact with our wormtable ::

	>>> qual_5_index = t.open_index('QUAL[5]')
	>>> # We can print the number of rows with QUAL scores between 0 and 100 using the counter function with our binned index
	>>> qual_5_counter = qual_5_index.counter()
	>>> for quality in range(0,101,5):
	>>> 	print q, qual_5_counter[q]



-------------------------------------------------
Examples ...
-------------------------------------------------

Along with the main program we have included a number of example scripts which 
will help you get started with Wormtable. These scripts highlight more of 
Wormtable's features and may be easily modified to suit your own purposes. If 
you want to read up on how these examples work and write your own scripts for 
Wormtable, full documentation can be found `here <http://jeromekelleher.github.io/wormtable/>` _. 

Count the distinct index values - *count-distinct.py*
-----------------------------------------------------

This script will take the name of any wormtable home directory and column which 
has been indexed and print each distinct value in that column and the number of 
times it occurs ::

	$ python count-distinct.py sample_wt/ REF

Transition-Transversion ratio - *ts-tv.py*
------------------------------------------
This uses a compound index of the reference nucleotide *REF* and the alternate 
nucleotide *ALT* to count the number of transitions (changes A<->G or C<->T) 
and transversions (A/G<->C/T). Using the counter feature this task can be very 
fast with Wormtable ::

	$ wtadmin add sample_wt/ REF+ALT #use this only if the REF+ALT index does not already exist.
	$ python ts-tv.py sample_wt/

High Quality SNPs - *hq-snps.py*
--------------------------------
In this example we provide a script that will return all the sites in your VCF 
that have a quality score over a particular minimum threshold. This script uses 
a QUAL index where QUAL scores have been grouped into bins of width 1 (QUAL[1]) ::

	$ wtadmin add sample_wt QUAL[1] #use this only if the QUAL[1] index does not already exist.
	$ python hq-snps.py -q 30 sample_wt/

Sliding window analysis of Genetic Diversity - *sliding-window.py*
-------------------------------------------------------------------
This script demonstrates how we can use the cursor feature of Wormtable to move 
through a file in windows and perform calculations on those windows. In this 
case we calculate the amount of genetic diversity that is present in each 
window using the alternate allele frequency (*AF* column) or by calculating the 
alternate allele frequency using the genotype calls in the sample columns.

