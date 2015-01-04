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
    key = record[0]
    value = record[1]
    trimmed = value[:-10]
    mr.emit_intermediate(trimmed, 1)

def reducer(key, list_of_values):
    # key: trimmed dna
    # value: list of occurrence counts
    mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
