import config
import requests
from urllib.parse import urlencode
from requests_toolbelt.utils import dump
from urllib.parse import quote

import json


def build_req(topics):
    post_fields = {"name": config.sink_mysql_connector_name, "config": {"connector.class":
                                                                         "io.confluent.connect.jdbc.JdbcSinkConnector",
                                                                     "tasks.max": 1,
                                                                     "topics": topics,
                                                                     "connection.url":
                                                                         "jdbc:mysql://" + config.sink_mysql[
                                                                             "end_point"] + ":3306/"
                                                                         + config.sink_mysql["database"] + "?user="
                                                                         + config.sink_mysql["user"] + "&password="
                                                                         + quote(config.sink_mysql["password"])
        , "pk.mode": "record_value", "pk.fields": "id", "insert.mode": "upsert", "auto.create": True}}
    header = {"content-type": "application/json"}
    print(table)
    print(json.dumps(post_fields))
    print("____________________XXXXXXXXXXXXXXXXXX__________________________")
    #return requests.post(config.connect_url, headers=header, data=json.dumps(post_fields))


topics = []
with open("table_tscolumn.tsv", "r") as f:
    for line in f:

        table = line.split(",")[0].strip()
        topic = config.prefix+table.lower()
        topics.append(topic)
        #data = dump.dump_all(req)

        #print(data.decode("utf-8"))

#print(",".join(topics))
build_req(",".join(topics))

