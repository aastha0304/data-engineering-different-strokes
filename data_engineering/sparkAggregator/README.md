#Spark Streaming

This spark-streaming application uses Confluent Platform componenents to process Kafka messages.

Build code by:

```
mvn package
```

Run by 

```
$SPARK_HOME/bin/spark-submit --class "aggregationEngine.sparkAggregator.AggregatorInit" --master spark://nik:7077 target/sparkAggregator.jar src/main/resources/config.properties
```