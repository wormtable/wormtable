Tutorial
========
This is the tutorial for getting started with wormtable. In this tutorial 
we'll build a wormtable using random numbers, and then use this table to
perform some statistical tests on these random numbers. This is a very
artificial example, of course.

Writing the table
-----------------
To write to a table, we must first define class that
specifies the columns in the table, and then generates the rows 
that we wish to store::

    import random
    import wormtable as wt

    def write_table(homedir, num_rows):
        uic = wt.IntegerColumn()
        gfc = wt.FloatColumn()
        with wt.open_writer(homedir, columns=[uic, gfc]) as tw:
            for j in range(num_rows):
                tw.update_row(uic, random.randint(-100, 100))
                tw.update_row(gfc, random.gauss(0, 100))
                tw.commit_row()
            
To begin, we define the columns that describe the data that we wish to 
store. In this example, we define two columns: a column for holding 
integer data and one for holding floating point information. Once 
we have defined the columns that we wish to store the data 
in, we then call the ``open_writer`` function to create an instance 
of the ``TableWriter`` class.

To write a table, we proceed row-by-row, first setting the values 
that we wish to assign to the columns and then commiting this 
row to the table, and then moving on to the next row. Values are 
assigned to columns in a row using the ``update_row`` method, which 
must be given a reference to the column we are updating. Once all of 
the columns have been assigned the correct values, we call 
``commit_row``; the row is then permenantly in the table, and 
cannot subsequently be modified.

Reading the table
-----------------
Once a table has been written, we can then read the values back 
from this table efficiently. In this example, we calculate
the mean of both columns and print it out::

    def get_mean(homedir):
        with wt.open_reader(homedir) as tr:
             sum_int = 0
             sum_float = 0.0
             n = 0
             for row in tr:
                sum_int += row[1]
                sum_float += row[2]
                n += 1
            print(sum_int / n, "\t", sum_float / n)



