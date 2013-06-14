Tutorial
========
This is the tutorial for getting started with wormtable.

Making a table
-----------------
In this section we make a table useing the command line tool 
wtadmin.

Reading a table
-----------------
Once a table has been built, it is very easy to read 
values back. For example, we can now print out all the 
rows::
        
    with wt.open_table("example.wt") as t:
        print("there are ", len(t), "rows in the table")
        for row in t:
            print(row)


More stuff.
