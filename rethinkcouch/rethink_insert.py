import rethinkdb as r
import json
import datetime
import sys
conn = r.connect(db='benchmark', port=32783)
dc_list = [[]]
batch = int(sys.argv[1])
i = int(sys.argv[2])
totaldiff = 0
with open('/mnt/modified_2016-05-04_11.log', 'r') as f:
        j = 0
        temp_list = []
        temp_list_len = 0
        for line in f:
            dc = json.loads(line)
            temp_list.append(dc)
            temp_list_len += 1
            #insert
            if temp_list_len == batch:
                a = datetime.datetime.now()
                r.table('impressions').insert(temp_list).run(conn)
                b = datetime.datetime.now()
                diff = (b-a).microseconds
                print(diff)
                totaldiff += diff
                #dc_list.append(temp_list)
                temp_list = []
                temp_list_len = 0
            if i > 0:
                j += 1
            if j == i > 0:
                break
print(totaldiff)
conn.close()