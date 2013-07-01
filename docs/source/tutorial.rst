========
Tutorial
========

------------
Introduction
------------
Wormtable is a method for storing and interacting with large scale datasets. It provides those familiar with python with the tools necessary to efficiently store, process and search datasets of essentially unlimited size. The data it stores is primarily tablular in format with many rows each with a fixed number of columns. Wormtable generates indexes of the information in these rows which allows the user to quickly access the information. The indexes are stored on the disk and can therefore be used repeatedly.

In this tutorial we will be taking you through the steps to convert a file to a wormtable, to index columns and we will demonstrate some basic uses of wormtable with examples.

The VCF format: Throughout this tutorial we will be using a *Variant Call Format* (VCF) table.  This is a common format for storing DNA sequence and polymorphism data from high throughput genome sequencing projects. In this format rows are individual positions of a genome and are identified by the chromosome they occur on and the position on that chromosome. A variety of other pieces of information are stored as metadata in the proceeding columns. We will explain the relevant columns to the examples as they arise. For more information please consult the full documentation on the `1000 genomes website <http://www.1000genomes.org/wiki/analysis/vcf4.0/>`_.


------------
Installation
------------

Wormtable requires Berkeley DB, which is available for all major platforms. Any recent version of Berkeley DB should work, but the various versions but we recommend using the DB 4.x series. Below we provide

Installing Berkeley DB
----------------------


Linux
*****
Wormtable is primarily developed on Linux and should work very well on any
modern Linux distribution. Installing Berkeley DB is very easy on Linux
distributions.

On Debian/Ubuntu use::

	   $ sudo apt-get install libdb-dev

and on Red Hat/Fedora use::

	   # yum install db4-devel

Other distributions and package managers should provide a similarly easy
option to install the DB development files.

Other Platforms
***************

On platforms that Berkeley DB is not available as part of the native packaging
system (or DB was installed locally because of non-root access)
there can be issues with finding the correct headers and libraries
when compiling ``wormtable``. For example, on FreeBSD we get something
like this::

	   $ python setup.py build
	   ... [Messages cut for brevity] ...
	   _wormtablemodule.c:3727: error: 'DB_NEXT_NODUP' undeclared (first use in this function)
	   _wormtablemodule.c:3733: error: 'DB_NOTFOUND' undeclared (first use in this function)
	   _wormtablemodule.c:3739: error: 'DistinctValueIterator' has no member named 'cursor'
	   _wormtablemodule.c:3739: error: 'DistinctValueIterator' has no member named 'cursor'
	   _wormtablemodule.c:3740: error: 'DistinctValueIterator' has no member named 'cursor'
	   error: command 'cc' failed with exit status 1

To remedy this we must set the
``LDFLAGS`` and ``CFLAGS`` environment variables to
their correct values. Unfortunately there is no simple method to do this
and some knowledge of where your system keeps headers and libraries
is needed.  On FreeBSD (after installing the ``db48`` package as above) we
might use::
	  
	    $ CFLAGS=-I/usr/local/include/db48 LDFLAGS=-L/usr/local/lib/db48 python setup.py build

Mac OS X
********

Berkeley DB can be installed from source on a mac, via macports or via homebrew

For MacPorts, to install e.g. v5.3 ::

    $ sudo port install db53
   
for more details of Berkely DB versions, see `here <https://www.macports.org/ports.php?by=category&substr=databases>`_.

Assuming Berkeley DB is installed with macports then we need to set the CFLAGS and LDFLAGS environment variables to use the headers and libraries in /opt::
 
    $ CFLAGS=-I/opt/local/include/db53 LDFLAGS=-L/opt/local/lib/db53/ python setup.py build
    $ sudo python setup.py install


Unix
*****

Most Unix systems provide Berkeley DB packages. For example, on FreeBSD
we have::

    # pkg_add -r db48

If necessary, Berkeley DB can be built from source and installed manually quite
easily.


Installing wormtable
----------------------

Once Berkeley DB has been installed we can build and install the ``wormtable`` module using the
standard Python `methods <http://docs.python.org/install/index.html>`_. For
example, using pip we have ::
	  
	   $ sudo pip install wormtable

Or, we can manually download the package, unpack it and then run::
	  
	   $ python setup.py build
	   $ sudo python setup.py install

Most of the time this will compile and install the module without difficulty.

It is also possible to download the latest development version of
``wormtable`` from `github <https://github.com/jeromekelleher/wormtable>`_.

 
---------------------------------
Convert a VCF to Wormtable format
---------------------------------

To convert a standard VCF file to WormTable format use the provided utility "vcf2wt". Like other utilities provided with Wormtable the description of command line options and arguments can be displayed by adding the "--help" flag after the utility ::

	$ vcf2wt --help

Building a Wormtable from a vcf file is easy using this command ::

	$ vcf2wt sample.vcf sample_DB

In this command the vcf file called sample.gz is read into a DB in the folder sample_DB. If the folder already exists you will have to use the "--force" (or -f) argument to tell vcf2wt to overwrite the old DB::

	$ vcf2wt -f sample.vcf sample_DB

The --progress (or -p) will keep you informed of the progress of the DB construction which will be helpful when monitoring larger jobs::

	$ vcf2wt -f -p sample.vcf sample_DB


---------------------------------
Building an index
---------------------------------

At this point your vcf has been converted into a Wormtable but in order to work with it you need to choose what columns you're interested in and 'index' those columns. The index provides a way to quickly and efficiently access information from the Wormtable based on the values in the indexed column. For example, if we are interested in knowing the DNA sequence of the reference genome (which is stored in the "*REF*" column) we can simply ask for the value of the *REF* column across a number of rows in the Wormtable that correspond to a piece of the reference genome.

To accomplish this we first need to index a few columns. Indexing columns, along with a number of other tools for administrating your wormtable, are done with the wtadmin utility. Amongst other features, wtadmin allows us to add indexes (wtadmin add), remove indexes (wtadmin rm) or list the columns already indexed (wtadmin ls). If we want to access the rows of our table according to their position in the genome we need to index the position column called "*POS*"::

	$ wtadmin add test POS

Here the "test" is the homedirectory which contains our Wormtable and POS is the name of the column for which we built an index. If you want to list the columns that are available to index use ::

 	$ wtadmin show test

Now that we have our wormtable built and POS indexed we can use python to interact with our new wormtable and index ::

	$ python
	import wormtable

	# We can open the wormtable using the open_table function
	t = wormtable.open_table('test')

	# Open the index that was built using wtadmin (see above)
	position_index = t.open_index('POS')


Note that if you have not already added the index using wtadmin add you won't be able to open the index in python. The Wormtable module offers a number of methods to interact with the data in your wormtable ::

	#print the minimum and maximum value of a index column
	position_index.get_min()
	position_index.get_max()
	
	#keys() returns an iterator to allow you to go through every value in your index in order.
	all_keys = [i for i in position_index.keys()]

Another convenient feature is the "cursor", which allows us to retrieve information from any column of our Wormtable based on the values in our indexed column. In this case, because we indexed the genomic position 'POS' we can return the reference nucleotide (the REF column) from the rows in a particular genomic window ::

	c = t.cursor(["REF"],position_index)

The names of the columns we want to retrieve are passed to the cursor as a list. We can set the minimum and maximum values for which the cursor will return columns ::

  c.set_min(8000000)
  c.set_max(8000500)

Now we can iterate through the *REF* columns from genomic positions with *POS* values between 8000000 and 8000500 ::

  for p in c:
      print p[0] #Note by default the cursor will return a tuple so take the first element returns a string 

However, you may have noticed this example isn't quite right. The *POS* column does not necessarily identify a single position in the genome because multiple chromosomes will have the same position. To deal with this we can can make compound indexes, another powerful feature of Wormtable. Compound indexes allow the user identify all combinations of multuple columns from the wormtable. For example we can make a compound index of chromosome (*CHROM*) and position (*POS*) to retrieve unique genomic positions. To add a compound column we can again use the wtadmin utility ::

	wtadmin add test CHROM+POS

Note that in this case the names of multiple columns are joined using "+" which indicates to wtadmin to make a compound index. It is important to realize that the order that the columns are listed matters. CHROM+POS does not equal POS+CHROM. With this new compound column we can specifiy a region of the genome unambiguously ::

	import wormtable
	t = wormtable.open_table('test')
	chrompos_index = t.open_index('CHROM+POS')
	c = t.cursor(["REF"],chrompos_index)
	c.set_min('1',8000000)
	c.set_max('1',8000500)
	for p in c:
		print p[0]

-----------------
Using the Counter 
-----------------
Another useful feature of wormtable is that the number of times a particular index value occurs is simple to retrieve. The counter is a dictionary-like object where the keys are index values which refer to the number of times that index occurs. For example, we can quickly and efficiently calculate the fraction of reference sites that are G or C (the GC content) ::

	ref_index = t.open_index('REF')
	ref_counts = ref_index.counter()
	GC_content = float(ref_counts['G'] + ref_counts['C']) / (ref_counts['T'] + ref_counts['A'] + ref_counts['G'] + ref_counts['C'])

----------------------------------
Using binned indexes
----------------------------------
Some of the columns in a VCF contain floats and can therefore have a huge number of distinct values. In these cases it may be useful to condense similar values into 'binned' indexes. For example, in the VCF the column which records the quality of row (QUAL column) can range from 0-1000 or more and you may not want to discern between a sites with quality of 50.1 from a site with quality of 50.2. Using wtadmin you can index a column binning indexes into equal sized bins like this ::

	$ wtadmin add test QUAL[5]

This will make a new index where all the QUAL values are grouped into bins of width 5. We can then use this binned index interact with our wormtable ::

	qual_5_index = t.open_index('QUAL[5]')
	# We can print the number of rows with QUAL scores between 0 and 100 using the counter function with our binned index
	qual_5_counter = qual_5_index.counter()
	for quality in range(0,101,5):
		print q, qual_5_counter[q]



-------------------------------------------------
Examples ...
-------------------------------------------------

Along with the main program we have included a number of example scripts which will help you get started with Wormtable. These scripts highlight more of Wormtable's features and may be easily modified to suit your own purposes. If you want to read up on how these examples work and write your own scripts for Wormtable, full documentation can be found `here <link_to_wormtable>` _. 


Count the distinct index values - *count-distinct.py*
-----------------------------------------------------

This script will take the name of any wormtable home directory and column which has been indexed and print each distinct value in that column and the number of times it occurs. 

Transition-Transversion ratio - *ts-tv.py*
-------------------------------------------------
This uses a compound index of the reference nucleotide *REF* and the alternate nucleotide *ALT* to count the number of transitions (changes A<->G or C<->T) and transversions (A/G<->C/T). Using the counter feature this task can be very fast with wormtable

High Quality SNPs - *hq-snps.py*
-------------------------------------------------
In this example we provide a script that will return all the sites in your VCF that have a quality score over a particular minumum threshold.

Sliding window analysis of Genetic Diversity - *sliding-window.py*
--------------------------------------------------------------------------------------------------
This script demonstrates how we can use the cursor feature of Wormtable to move through a file in windows and perform calculations on those windows. In this case we calculate the amount of genetic diversity that is present in each window using the alternate allele frequency (*AF* column) or by calculating the alternate allele frequency using the genotype calls in the sample columns.