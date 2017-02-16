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
