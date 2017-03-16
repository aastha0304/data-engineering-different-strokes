This is a different setup for Kafka-connect since there had to be made some tweakings for offset-timeouts for large tables. 

Build the docker images in this folder using 

```
docker build -t kafka-con .
```
and run using 

```
docker run -d -p8083:8083 --name kafka-connect kafka-con
```
Then create a jdbc source from table(s) using 

```
curl -X POST -H "Content-Type: application/json" \
  --data '
{ "name": "leagueMaster2", "config": { "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", "batch.max.rows":1000, "tasks.max": 4, "connection.url": "jdbc:mysql://<host>:<port>/<db>?user=<username>&password=<password>&defaultFetchSize=1000&useCursorFetch=true", "mode": "timestamp+incrementing", "incrementing.column.name": "id", "timestamp.column.name": "UpdatedAt", "topic.prefix": "r2-", "poll.interval.ms":300000, "table.whitelist": "<table list>"  } }' \
  http://$CONNECT_HOST:28083/connectors
``` 





