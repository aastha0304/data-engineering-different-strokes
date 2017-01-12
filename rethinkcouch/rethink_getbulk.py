import rethinkdb as r
import json
import datetime
import sys
totaldiff = 0

batch = int(sys.argv[1])
i = int(sys.argv[2])
conn = r.connect(db='benchmark', port=32783)

with open('/mnt/modified_2016-05-04_11.log', 'r') as f:
        j = 0
        temp_list = []
        temp_list_len = 0
        for line in f:
            dc = json.loads(line)
            temp_list.append(dc['id'])
            temp_list_len += 1
            #insert
            if temp_list_len == batch:
                a = datetime.datetime.now()
                resultset = r.table('impressions').get_all(r.args(temp_list)).run(conn)
                #print(len(resultset))
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
# r.expr(['92eebf16-a249-4a93-a654-e1262f1bc5c0', '2b184e9e-5ca3-4bc4-9918-cc43a44e4cbe']).map(r.table('impressions').get(r.row)).run(conn)