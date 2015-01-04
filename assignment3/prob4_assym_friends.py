import MapReduce
import sys

"""
Create count of friends for a person 
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    name = record[0]
    friend = record[1]
    mr.emit_intermediate(name, friend)
    mr.emit_intermediate(friend, name)

def reducer(key, list_of_values):
    # key: name of person
    # value: list of occurrence counts
    dist_list = list(set(list_of_values))
    for v in dist_list:
      mr.emit((key, v))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
