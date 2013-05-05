Performance tuning 
==========================
Wormtable can be deployed on any Unix system supporting Berkeley DB, and 
so should be very portable. It has been tested on big and little endian 
systems, with 32 and 64 bit word sizes. It should work on any 
Unix system with a C compiler. At very large scales, however, at very 
large scales the properties of different platforms become significant
and some tuning of parameters may be necessary to obtain the best 
performane. Wormtable is developed on Linux, and is primarily 
aimed at Linux systems with large amounts of RAM and substantial 
I/O capabilities. As a result, some of the advice below will be 
Linux specific and suggest what may seem like very large cache 
sizes.  Howevever, the underlying issues 
are likely to affect any modern Unix and machines with 
smaller available resources, so may provide a useful 
starting point for tuning performance on these systems.
    
Cache
-----
The single most important parameter to tune for performance in wormtable 
is the amount of cache. The general rule for cache is "more is 
better", but with some important caveats. The considerations are 
somewhat different for writing and reading tables, so we discuss 
these seperately.

As a rule of thumb, it 
is a good idea to set aside half of available RAM for cache 
when writing new tables and indices
So, for example, in a system with 16GB of RAM, a good amount of 
cache to allocate would be 8GB. This may seem like a very large 
amount of memory to dedicate to cache, but the more of the 
underlying Berkeley DB that fits into the cache the better 
performance will be as we avoid the costly process of writing 
pages to disc which may need to be read back in later.
Ideally, we would like to fit the entire DB into memory
while we are generating it, which means we only need to 
write each page to disc once. 
This situation often occurs when we are creating indexes.

When reading tables, we rarely need as much cache as when 
we are writing them and specifiying too much cache may 
have a negative impact on overall system performance 
if the system is busy. The amount of cache to allocate 
to different indexes and to the main table is a subtle issue
and depends on the type of queries that you run. 

[Fill this in with examples and discussion of the issues.]

Linux filesystems and kernel parameters
---------------------------------------
For very large tables substantial space savings and performance 
benifts can be made by choosing filesystems and parameters 
well. Good performance has been obtained with the ext4 filesystem
using the "largefile" options, and the xfs filesystem has 
shown excellent performance when reading and writing large 
tables.

A very important factor for performance when writing large 
tables in Linux is the "swappiness" kernel parameter. This parameter 
controls how agressively the kernel tries to swap memory pages 
that are mapped to processes to free up pages for IO purposes. 
This value is typically set to 60, which is much too high when 
building large tables using an appropriate cache size. The 
problem happens when the cache has filled up and Berkeley DB 
starts to flush pages to disc. Eventually, the kernel runs out 
of IO buffers and starts looking more pages. With a high 
"swappiness" value, the kernel will then swap pages that are 
mapped to process and haven't been accessed for some time and 
use this memory as IO cache. Unfortunately, this means that 
pages that are used by Berkeley DB will be swapped out too,
leading to severe performance degradation. This situation 
can be avoided by setting the swappiness parameter to a 
small value (0 is perfectly reasonable for a system with 
large amounts of RAM).

[Finish up this discussion].


