#Kafka Connect

Kafka connect supports multiple connectors to stream events to/from sources kike MySql, Cassandra, HDFS etc. This is a demo of reading tables from mysql and dumping to redshift using Kafka connect JDBS source connector.

Follow [these](http://docs.confluent.io/3.1.2/cp-docker-images/docs/tutorials/connect-avro-jdbc.html) instructions to get the required driver jars for different connectors. (You'll not need for postgres and sqlite but will do for Mysql). 

For JDBC source, needed a different setup (covered in separate folder)

###MYSQL Sink
Run dockerised kafka connect by 

```
docker run -d   --name=kafka-connect   --net=host   -e CONNECT_BOOTSTRAP_SERVERS=172.31.26.136:9092,172.31.26.137:9092,172.31.26.138:9092   -e CONNECT_REST_PORT=28083   -e CONNECT_GROUP_ID="kafka-connect-d11"   -e CONNECT_CONFIG_STORAGE_TOPIC="connect-config"   -e CONNECT_OFFSET_STORAGE_TOPIC="connect-offset"   -e CONNECT_STATUS_STORAGE_TOPIC="connect-stats"   -e CONNECT_KEY_CONVERTER="io.confluent.connect.avro.AvroConverter"   -e CONNECT_VALUE_CONVERTER="io.confluent.connect.avro.AvroConverter"   -e CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL="http://172.31.25.115:8081"   -e CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL="http://172.31.25.115:8081"   -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter"   -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"   -e CONNECT_REST_ADVERTISED_HOST_NAME=172.31.25.115   -e CONNECT_LOG4J_ROOT_LOGLEVEL=WARN   -v /mnt/confluent_platform/kafka-connect/jars:/etc/kafka-connect/jars   -v /mnt/confluent_platform/kafka-connect/file:/tmp/quickstart   confluentinc/cp-kafka-connect:latest
```

Here "/mnt/confluent_platform/kafka-connect/file" and "/mnt/confluent_platform/kafka-connect/jars" are host files and "tmp/quickstart" and "/etc/kafka-connect/jars" are on docker host so replace accordingly.

Kafka connect is running. Next point this to get data from mysql using this call

```
curl -X POST   -H "Content-Type: application/json" \
--data '{ "name": "leagueMaster2-sink6", "config": { "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "tasks.max": 4, "topics":"r2-Dream11_LeagueMaster", "connection.url": "jdbc:mysql://<host>:<port>/<db>?user=<username>&password=<password>&defaultFetchSize=1000&useCursorFetch=true", "auto.create":"true", "pk.mode": "record_value", "pk.fields":"id"} }' http://$KAFKA_CONNECT_HOST:28083/connectors
```

Remember to escape "%" in connection url by replacing % with %25 or url encode in program. 

"timestamp+incrementing" mode is the most reliable mode. Give the auto-incrementing column if you have incrementing mode on, and the modified timestamp column if you have the timestamp mode on. Use "table.whitelist" option for selecting tables, or all tables will be streamed. 

Use mysql-connector-java-5.1.41-bin.jar or whichevr is latest or else you might hit issues of data conversion to long and deserialization will suffer.

###Redshift Sink

Redshift configuration is given in redshift-sink.properties

For some reason, docker container Kafka connect could not work so using connect-standalone with this command.

```
connect-standalone /etc/schema-registry/connect-avro-standalone.properties redshift-sink.properties 
```

And it worked :)

