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
    if record[0] == 'a':
      for i in range(0,5):
        mr.emit_intermediate((record[1],i), record)
    else: # this record is from matrix 'b'
      for i in range (0,5):
        mr.emit_intermediate((i,record[2]), record)

def reducer(key, list_of_values):
    # key: trimmed dna
    # value: list of occurrence counts
    sum = 0;
    mult_list = [[0,0], [0,0], [0,0], [0,0], [0,0]]
    for item in list_of_values:
      if item[0] == 'a':
        mult_list[item[2]][0] = item[3]
      else:
        mult_list[item[1]][1] = item[3]
    for i in range(0,len(mult_list)):
      sum += (mult_list[i][0] * mult_list[i][1])
    mr.emit((key[0], key[1], sum))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
