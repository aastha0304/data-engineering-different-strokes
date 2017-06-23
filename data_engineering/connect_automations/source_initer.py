import MySQLdb
import config
import requests
from urllib.parse import urlencode
from requests_toolbelt.utils import dump
from urllib.parse import quote

import json


def build_query(table, tscolumn):
    cur.execute(col_qry+table)
    qry = "select "+ ", ".join(columns[0].lower() for columns in cur.fetchall()) + \
          ", DATE_FORMAT("+tscolumn+", \"%Y%m%d\") as partition_by from " + table
    return qry


def fetch_mode(qry):
    if " id," in qry:
        return "timestamp+incrementing"
    else:
        return "timestamp"


def build_req(qry, tscolumn, table):
    post_fields = {"name": table.lower()+config.src_connector_name, "config": {"connector.class":
                                                      "io.confluent.connect.jdbc.JdbcSourceConnector",
                                                      "batch.max.rows":1000, "tasks.max": 1, "connection.url":
                                                          "jdbc:mysql://"+config.source_db["end_point"]+":3306/"
                                                          +config.source_db["database"]+"?user="
                                                          +config.source_db["user"]+"&password="
                                                          +quote(config.source_db["password"])
                                                          +"&zeroDateTimeBehavior=round&defaultFetchSize=1000"
                                                           "&useCursorFetch=true",
                                                      "mode": fetch_mode(qry),
                                                      "timestamp.column.name": tscolumn,
                                                                             "topic.prefix": config.prefix+table.lower(),
                                                      "poll.interval.ms": 300000, "query": qry} }

    if " id," in qry:
        post_fields["config"]["incrementing.column.name"] = "id"
    header = {"content-type": "application/json"}
    print(table)
    print(json.dumps(post_fields))
    print("____________________XXXXXXXXXXXXXXXXXX__________________________")
    #return requests.post(config.connect_url, headers=header, data=json.dumps(post_fields))

db = MySQLdb.connect(host=config.source_db["end_point"],    # your host, usually localhost
                     user=config.source_db["user"],         # your username
                     passwd=config.source_db["password"],  # your password
                     db=config.source_db["database"])
cur = db.cursor()

col_qry = "show columns from "

with open("table_tscolumn1.tsv", "r") as f:
    for line in f:
        table = line.split(",")[0].strip()
        tscolumn = line.split(",")[1].strip().lower()
        qry = build_query(table, tscolumn)
        #print(qry)
        req = build_req(qry, tscolumn, table)
        #data = dump.dump_all(req)
        #print(data.decode("utf-8"))

db.close()