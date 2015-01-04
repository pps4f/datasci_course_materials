import MapReduce
import sys

"""
SQL-like join between two tables 
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    orderid = record[1]
    recordvalues = record
    mr.emit_intermediate(orderid, recordvalues)
    # words = value.split()
    # for w in words:
      # mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = []
    order_record = []
    for v in list_of_values:
      if v[0] == 'order':
        order_record = v
    for u in list_of_values:
      if u[0] == 'line_item':
        output = order_record + u;
        mr.emit(output)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
