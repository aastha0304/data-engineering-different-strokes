from couchbase.bucket import Bucket
import sys
from multiprocessing import Process, Manager
import json
import datetime


def multi_process_get(i, temp_list, return_dict):
    bucket = Bucket('couchbase://localhost/default', timeout=1000)
    a = datetime.datetime.now()
    resultset = bucket.set_multi(temp_list)
    print('len .....', len(resultset))
    b = datetime.datetime.now()
    diff = (b-a).microseconds
    print('diff ....', diff)
    return_dict[i] = diff


def main():
    totaldiff = 0
    batch = int(sys.argv[1])
    i = int(sys.argv[2])
    dc_list = [[]]
    manager = Manager()
    return_dict = manager.dict()
    jobs = []
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
                dc_list.append(temp_list)
                temp_list = {}
                temp_list_len = 0

            if i > 0:
                j += 1
            if j == i > 0:
                break
    dc_list_len = len(dc_list)
    for i in range(dc_list_len):
        p = Process(target=multi_process_get, args=(i, dc_list[i], return_dict))
        jobs.append(p)
        p.start()
    result = []
    for job in jobs:
        job.join()
    for x in return_dict.values():
        totaldiff+=x
    print('totaldiff ....', totaldiff)
if __name__ == '__main__':
    main()