[**Confluent Platform**] (<http://docs.confluent.io/3.1.2/>) is a eventing framework from Confluent Inc that leverages **Kafka** to pub/sub messages form different sources, validate them, process them and forward to any data store. 

We are using [**Schema Registry**] (<http://docs.confluent.io/3.1.2/schema-registry/docs/index.html>) and [**Rest Proxy**]  (<http://docs.confluent.io/3.1.2/kafka-rest/docs/index.html>) to talk to our extant Kafka and Zookeeper setup. Schema Registry validates all events for a topic and Rest Proxy provides a standard event end point.

These services are set up via **Docker**. You may need to [install](http://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html) docker. 

Provide a config file for *docker\_schema\_registry.sh* and run like this
```
sh docker_schema_registry.sh <your properties file>
```

Running Kafka rest from docker can follow the same way but has been done via [command](http://docs.confluent.io/3.1.2/cp-docker-images/docs/quickstart.html) straightup.
  



