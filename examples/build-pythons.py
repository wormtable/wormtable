"""
Example of building a wormtable.
"""

import wormtable as wt

def make_pythons():
    t = wt.Table("pythons.wt")
    t.add_id_column(1)
    t.add_char_column("name")   
    t.add_uint_column("born")   
    t.add_uint_column("writer")   
    t.add_uint_column("actor")   
    t.add_uint_column("director")   
    t.add_uint_column("producer")   
    t.open("w")
    rows = [
        ["John Cleese", 1939, 60, 127, 0, 43],
        ["Terry Gilliam", 1940, 25, 24, 18, 8], 
        ["Eric Idle", 1943, 38, 74, 7, 5], 
        ["Terry Jones", 1942, 50, 49, 16, 1], 
        ["Michael Palin", 1943, 58, 56, 0, 1], 
        ["Graham Chapman", 1941, 46, 24, 0, 2] 
    ]
    for r in rows:
        t.append([None] + r)
    t.close()

make_pythons()


