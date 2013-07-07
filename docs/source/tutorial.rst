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

Throughout this tutorial, code lines beginning ``$`` imply a bash shell and 
``>>>`` imply a python shell.

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

In the following examples we will be working with the following example vcf file with 
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
indivudal samples have been represented as separate columns and named as 
[COLUMN].[FIELD]. This allows the user to create indexes on individual fields from these
compound columns.

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
	>>> table = wormtable.open_table('sample_wt') # open the wormtable
	>>> position_index = table.open_index('POS')  # open the index on POS

Note that if you have not already added the index using wtadmin add you will not 
be able to open the index in python. Also, worth noting is that, like cache sizes,
when building tables or adding indexes we can assign memory to both the table 
and index when we open them by including the cache size as a second argument in 
opentable() or open_index(). For more details see 
`Performance tuning <http://jeromekelleher.github.io/wormtable/performance.html>`_. 

The Wormtable python module offers a number of methods to interact with an index::

	>>> # Print the minimum and maximum value of an index
	>>> position_index.get_min()
	14370L
	>>> position_index.get_max()
	1234567L
	>>> # Use keys() to iterate through sorted value in the index
	>>> for i in position_index.keys():
	... 	print(i) 
	... 
	14370
	17330
	1110696
	1230237
	1234567

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

	>>> c.set_min(1)
	>>> c.set_max(1150000)

and then iterate through positions in this range (1-1150000), returning 
the *REF* column for each row of the table::

	>>> for p in c: 
	... 	print(p[0]) 
	... 
	G
	T
	A

Note that by default the cursor will return a tuple and we just 
print the first element here. It is also worth noting that like other 
ranges in Python, the maximum value is not included. For example, 
1 to 100 would return 1 to 99 and not include 100.

-------------------------
Creating compound indexes
-------------------------
With multiple chromsomes, the example above could give multiple values for each position 
because the *POS* column is not normally a unique identifier of genomic position and our 
cursor will iterate over positions matching the range specified from multiple 
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
(chromosome 1, positions 1 to 1150000) unambiguously and iterate 
through rows in this region, printing CHROM, POS and REF for each::

	>>> import wormtable
	>>> table = wormtable.open_table('sample_wt')
	>>> chrompos_index = table.open_index('CHROM+POS')
	>>> c = table.cursor(['REF'], chrompos_index)
	>>> c.set_min('20',1)
	>>> c.set_max('20',1150000)
	>>> for p in c:
	... 	print(p[0])
	... 
	G
	T
	A

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

Then in python: ::

	>>> import wormtable
	>>> table = wormtable.open_table('sample_wt')
	>>> ref_index = table.open_index('REF')
	>>> ref_counts = ref_index.counter()
	>>> gc = ref_counts['G'] + ref_counts['C']
	>>> tot = gc + ref_counts['T'] + ref_counts['A']
	>>> float(gc) / float(tot)
	0.25

--------------------
Using binned indexes
--------------------
Some columns in a VCF contain floats and can therefore have a huge number of 
distinct values. In these cases it is useful to condense similar values into 
'binned' indexes. For example, in a VCF the column which records the quality of 
row (QUAL column) is a float which may range from 0 to 10,000 (or more). For the 
purposes of filtering on this column (i.e. creating an index) it may not be 
necessary to discern between sites with quality of 50.1 from sites with quality 
of 50.2. Using wtadmin you can index a column binning indexes into equal sized 
bins of size n like this ::

	$ wtadmin add sample_wt QUAL[n]

where n is an integer. This will make a new index on QUAL where all the QUAL 
values are grouped into bins of size n. We can then use this binned index 
to interact with our wormtable and print the number of rows matching QUAL scores 
in bins between 0 and 70 using the counted function (e.g. for a bin size of 5)::

	$ wtadmin add sample_wt QUAL[5]
	
	>>> qual_5_index = table.open_index('QUAL[5]')
	>>> qual_5_counter = qual_5_index.counter()
	>>> for q in range(0,70,5):
	...  	print("%i\t%i" %(q, qual_5_counter[q]))
	... 
	0	1
	5	0
	10	0
	15	0
	20	0
	25	1
	30	0
	35	0
	40	0
	45	1
	50	1
	55	0
	60	0
	65	1

Note, as above the upper bound (70) is exclusive.

--------
Examples
--------
Along with the main program we have included a number of example scripts which 
will help you get started with using Wormtable. In the next few examples we will 
demonstrate the concepts in these examples. The full scripts are available should 
you want to use or modify the example scripts for your own purposes 
If you want write your own scripts for Wormtable, full documentation can be found 
`here <http://jeromekelleher.github.io/wormtable/>`_. 

Count the keys in an index - *count-keys.py*
-----------------------------------------------------
The idea of this script is to implement a simple counter for a named wormtable directory 
(homedir) and an existing index (index) and prints out counts for each key in the index ::

	>>> import wormtable as wt
	>>> def count_distinct(homedir, index):
	... 	with wt.open_table(homedir) as t, t.open_index(index) as i:
	... 		table = [[k,v] for k,v in i.counter().items()]
	...			assert(len(t) == sum(r[1] for r in table)) # the sum of the counts should match the number of rows
	... 	return table
	...
	>>> ref_table = count_distinct('sample_wt', 'REF')
	>>> for r in ref_table:
	... 	print("%s\t%i" %(r[0], r[1]))
	... 
	A       1
	G       1
	GTCT    1
	T       2

Alternatively you can use the python script provided in the examples folder ::

	$ python count-distinct.py sample_wt REF
	A       1
	G       1
	GTCT    1
	T       2

Transition-Transversion ratio - *count-ts-tv.py*
------------------------------------------
This example uses a compound index of the reference nucleotide *REF* and the alternate 
nucleotide *ALT* to count the number of transitions (changes A <-> G or C <-> T) and 
transversions (A or G <-> C or T). Using the counter feature this task can be very fast 
with Wormtable. First we use Python's itertools to generate a list of all possible 
single bases changes (ie all pairs of A,C,G and T). We then count the number of
instances of each change in our data ::

	>>> import wormtable
	>>> from itertools import permutations
	>>> def count_Ts_Tv(homedir):
	... 	""" 
	... 	Count number of of transitions and transversions using an index on REF+ALT
	... 	"""
	... 	subs = [p for p in permutations([b'A',b'C',b'G',b'T'], 2)]
	... 	bases = {b'A':'purine', b'G':'purine', b'C':'pyrimidine', b'T':'pyrimidine'}
	... 	t = wormtable.open_table(homedir)
	... 	i = t.open_index("REF+ALT")
	... 	Ts, Tv = 0, 0
	... 	c = i.counter()
	... 	for s in subs:
	... 		if bases[s[0]] == bases[s[1]]: 
	... 			Ts += c[s] 
	... 		else: 
	... 			Tv += c[s] 
	... 	i.close()
	... 	t.close()
	... 	return Ts, Tv
	...
	>>> count_Ts_Tv('sample_wt')
	(1L, 1L)

Similar to the previous example we have provided a script for doing this that can be 
called form the commandline ::

	$ wtadmin add sample_wt REF+ALT # in case index does not already exist.
	$ python ts-tv.py sample_wt
	ts: 1 tv: 1

High Quality SNPs - *find-hq-snps.py*
--------------------------------
In this example we provide a script that will return all the sites in your VCF 
that have a quality score over a particular minimum threshold. This script uses 
a QUAL index where QUAL scores have been grouped into bins of width 1 (QUAL[1]) 
::

	>>> import wormtable
	>>> def hq_snps(homedir, minq, cols):
	... 	"""
	... 	minq is the minimum quality that determines a high quality site
	... 	cols is a list of the columns from the VCF that you want to return
	... 	"""
	... 	t =  wormtable.open_table(homedir)
	... 	i = t.open_index("QUAL[1]")
	... 	cursor = t.cursor(cols, i)
	... 	cursor.set_min(minq)
	... 	cursor.set_max(i.get_max())
	... 	for row in cursor:
	... 		print "\t".join([str(i) for i in row])
	... 
	>>> hq_snps('sample_wt',30, ['CHROM', 'POS', 'REF', 'ALT', 'QUAL'])
	20      1230237 T               47.0
	20      1234567 GTCT    G,GTACT 50.0

or using the provided python script ::

	$ wtadmin add sample_wt QUAL[1] # in case index does not already exist.
	$ python hq-snps.py -q 30 sample_wt
	20      1230237 T               47.0
	20      1234567 GTCT    G,GTACT 50.0

