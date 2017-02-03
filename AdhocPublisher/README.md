This is a code that takes existing Kafka data, consumes it and produces it in compliance to predefined schema.
Currently it is working on only user balance change events, hence it takes onlu **user-deposited** and **first-deposit** events.

This is the schema decided for user balnce change events 
###Key:
```
{"schema": "{"type":"long"}"}
```
###Value
```
{"schema": "{"type":"record","name":"balanceTxns","fields":[{"name":"balanceTxns","type":[{"type":"record","name":"userDeposited","fields":[{"name":"userId","type":"long"},{"name":"amount","type":"float"},{"name":"timestamp","type":"long"}]},{"type":"record","name":"firstDeposit","fields":[{"name":"userId","type":"long"},{"name":"amount","type":"float"},{"name":"timestamp","type":"long"}]}]}]}"}
```
The code is a maven project.

Build fatjar by running 

```
mvn package
```

Run by 
```
mvn exec:java -Dexec.mainClass=POCConfluentProd.javaProd.AdhocConProd  -Dexec.args="<Path to a config>"
```

You can test sample output by this curl request

```
curl -X GET -H "Accept: application/vnd.kafka.avro.v1+json" http://ec2-54-196-240-237.compute-1.amazonaws.com:8082/consumers/my_avro_consumer/instances/my_consumer_instance/topics/balanceTxns
```
This is NOT the schema registry url, this is the rest-proxy url. It provides end-points for pub/sub that can be used.

Other configs are in the resources path of the project (src/main/resources/config.properties)