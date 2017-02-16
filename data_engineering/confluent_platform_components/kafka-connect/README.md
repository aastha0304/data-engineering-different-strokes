#Kafka Connect

Kafka connect supports multiple connectors to stream events to/from sources kike MySql, Cassandra, HDFS etc. This is a demo of reading tables from mysql and dumping to redshift using Kafka connect JDBS source connector.

Follow [these](http://docs.confluent.io/3.1.2/cp-docker-images/docs/tutorials/connect-avro-jdbc.html) instructions to get the required driver jars for different connectors. (You'll not need for postgres and sqlite but will do for Mysql). 

Run dockerised kafka connect by 

```
docker run -d \
  --name=kafka-connect-avro \
  --net=host \
  -e CONNECT_BOOTSTRAP_SERVERS=172.31.26.136:9092,172.31.26.137:9092,172.31.26.138:9092 \
  -e CONNECT_REST_PORT=28083 \
  -e CONNECT_GROUP_ID="quickstart-avro" \
  -e CONNECT_CONFIG_STORAGE_TOPIC="quickstart-avro-config" \
  -e CONNECT_OFFSET_STORAGE_TOPIC="quickstart-avro-offsets" \
  -e CONNECT_STATUS_STORAGE_TOPIC="quickstart-avro-status" \
  -e CONNECT_KEY_CONVERTER="io.confluent.connect.avro.AvroConverter" \
  -e CONNECT_VALUE_CONVERTER="io.confluent.connect.avro.AvroConverter" \
  -e CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL="http://ip-172-31-25-115:8081" \
  -e CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL="http://ip-172-31-25-115:8081" \
  -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_REST_ADVERTISED_HOST_NAME=$HOSTNAME \
  -e CONNECT_LOG4J_ROOT_LOGLEVEL=DEBUG \
  -v /mnt/kafka-connect/file:/tmp/quickstart \
  -v /mnt/kafka-connect/jars:/etc/kafka-connect/jars \
  confluentinc/cp-kafka-connect:3.1.1
```

Here "/mnt/kafka-connect/file" and "/mnt/kafka-connect/jars" are host files and "tmp/quickstart" and "/etc/kafka-connect/jars" are on docker host so replace accordingly.

Kafka connect is running. Next point this to get data from mysql using this call

```
curl -X POST   -H "Content-Type: application/json" \
--data '{ "name": "test-mysql-jdbc", "config": { "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", "tasks.max": 1, \
"connection.url": "jdbc:mysql://$MYSQL_HOST:$MYSQL_PORT/$DB?user=$USER&password=$PASSWORD", \
"mode": "timestamp+incrementing", "incrementing.column.name": "id", "timestamp.column.name": "modified", "topic.prefix": "test-mysql-jdbc-", "poll.interval.ms": 1000, "table.whitelist": "test_kafka_connect,test_kafka_jdbc"  } }' http://$KAFKA_CONNECT_HOST:28083/connectors
```

This is standalone mode of KC. Remember to escape "%" in connection url by replacing % with %25 or url encode in program. 

"timestamp+incrementing" mode is most reliable mode. Give the auto-incrementing column if you have incrementing mode on, and the modified timestamp column if you have the timestamp mode on. Use "table.whitelist" option for selecting tables, or all tables will be streamed. 