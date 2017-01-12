from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
import sys
import json
import datetime
conn = Couchbase.connect(bucket='default', host='localhost', timeout=60)
dc_list = [[]]
totaldiff = 0

batch = int(sys.argv[1])
i = int(sys.argv[2])
try:
    with open('/mnt/modified_2016-05-04_11.log', 'r') as f:
        j = 0
        temp_list = {}
        temp_list_len = 0
        for line in f:
            dc = json.loads(line)
            temp_list[dc['id']] = dc
            temp_list_len += 1
            #insert
            if temp_list_len == batch:
                a = datetime.datetime.now()
                conn.set_multi(temp_list)
                b = datetime.datetime.now()
                diff = (b-a).microseconds
                print(diff)
                totaldiff += diff
                #dc_list.append(temp_list)
                temp_list = {}
                temp_list_len = 0
            if i > 0:
                j += 1
            if j == i > 0:
                break
    print(totaldiff)
except CouchbaseError as e:
    print(e)
