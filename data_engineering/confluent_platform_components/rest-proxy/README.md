#Rest proxy

[This](http://docs.confluent.io/3.1.2/kafka-rest/docs/intro.html#produce-and-consume-avro-messages) is the rest api for listing topics, producing and consuming messages from Kafka. It provides a language agnostic way to leverage Kafka.

A docker image for rest proxy is run by

```
docker run -d --net=host  --name=kafka-rest -e KAFKA_REST_HOST_NAME=$HOSTNAME -e KAFKA_REST_ZOOKEEPER_CONNECT=172.31.18.67:2181,172.31.18.68:2181,172.31.18.69:2181 -e KAFKA_REST_LISTENERS=http://$HOSTNAME:8082 -e KAFKA_REST_SCHEMA_REGISTRY_URL=http://$HOSTNAME:8081,http://ip-172-31-21-18:8081 confluentinc/cp-kafka-rest:3.1.1
```
