package spark_aggregator.league.sink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.util.retry.RetryBuilder;

import rx.Observable;
import rx.functions.Func1;
import scala.Tuple2;
import spark_aggregator.league.UsableColumns;
import spark_aggregator.league.utils.Constants;

public class CouchbaseSink implements Sink, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Properties properties;
	private String topic;
	final String fullLeaguesKey;
	final String offsetKey;
	public CouchbaseSink(Properties properties){
		this.properties = properties;
		this.topic = (String)properties.get("topic");
		fullLeaguesKey = new StringBuffer()
				.append(this.topic)
				.append(":")
				.append(Constants.FULLLEAGUES_KEY).toString();
		offsetKey = new StringBuffer()
				.append(this.topic)
				.append(":")
				.append(Constants.PARTITION_KEY).toString();
	}
	
	@Override
	public Map<TopicPartition, Long> getAndUpdateOffsets() {
		Map<TopicPartition, Long> res = new HashMap<>();
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		if(bucket.exists(this.topic+":"+Constants.PARTITION_KEY)){
			JsonDocument json = bucket.get(this.topic+":"+Constants.PARTITION_KEY);
			JsonObject x = json.content();
			if(x!=null){
				Map<String, Object> m = x.toMap();
				Set<Entry<String, Object>> entries = m.entrySet();
				for(Entry<String, Object> entry:entries){
					@SuppressWarnings("unchecked")
					HashMap<String, Integer> v = (HashMap<String, Integer>) entry.getValue();
					res.put(new TopicPartition(this.topic, Integer.parseInt(entry.getKey())), 
							v.get(Constants.UNTILOFFSET).longValue());		
				}
			}
		}
		// Disconnect and clear all allocated resources
		cluster.disconnect();
		return res;
	}
	@SuppressWarnings("unchecked")
	private void addAggregates(final Bucket bucket, List<String> queries, final Map<String, JsonDocument> map){
		Observable.from(queries)
		        .flatMap(new Func1<String, Observable<JsonDocument>>() {
		            @Override
		            public Observable<JsonDocument> call(String id) {
			            	JsonDocument old = bucket.get(id);
			            	if(old!=null){
			            		
			            		JsonDocument jsonDoc = map.get(old.id());
			            		JsonDocument newDoc = JsonDocument.create(jsonDoc.id(), JsonObject.empty()
			            				.put(Constants.COUNT, old.content().getLong(Constants.COUNT)
			            						+jsonDoc.content().getLong(Constants.COUNT))
										.put(Constants.SUM, old.content().getDouble(Constants.SUM)
												+jsonDoc.content().getDouble(Constants.SUM)));
			            		return bucket.async().replace(newDoc);
			            	}else{
			            		return bucket.async().insert(map.get(id));
			            	}
		            }
		        })
		        .retryWhen(
						  RetryBuilder
						    //will limit to the relevant exception
						    .anyOf(CASMismatchException.class, DocumentAlreadyExistsException.class)
						    //will retry only 5 times
						    .max(5)
						    //delay doubling each time, from 100ms to 2s
						    .delay(Delay.linear(TimeUnit.MILLISECONDS, 2000, 100, 2))
						  .build()
						)
		        .toList()
		        .toBlocking()
			    .singleOrDefault(null);
	}
	@SuppressWarnings("unchecked")
	private void addFullLeagues(final Bucket bucket, final Set<Long> fullLeagues){
		final JsonArray currentFullLeagues = JsonArray.from(fullLeagues.toArray());
		Observable
				.just(fullLeaguesKey)
				.flatMap(new Func1<String, Observable<JsonArrayDocument>>(){

					@Override
					public Observable<JsonArrayDocument> call(String id) {
						if(bucket.exists(id)){
							JsonArrayDocument old = bucket.get(id, JsonArrayDocument.class);
							old.content().add(currentFullLeagues);
							return bucket.async().replace(old);
						}else{
							return bucket.async().insert(JsonArrayDocument.create(fullLeaguesKey, 
									JsonArray.from(currentFullLeagues)));
						}
					}
					
				})
				.retryWhen(
						  RetryBuilder
						    .anyOf(CASMismatchException.class, DocumentAlreadyExistsException.class)
						    .max(5)
						    .delay(Delay.linear(TimeUnit.MILLISECONDS, 2000, 100, 2))
						  .build()
						)
				.last()
				.toBlocking()
			    .singleOrDefault(null);
	}
	@SuppressWarnings("unchecked")
	private void updatePartitions(final Bucket bucket, final OffsetRange offset){
		final String partitionId = String.valueOf(offset.partition());

		Observable
		.just(offsetKey)
		.flatMap(new Func1<String, Observable<JsonDocument>>(){

			@Override
			public Observable<JsonDocument> call(String id) {
				if(bucket.exists(id)){

					JsonDocument old = bucket.get(offsetKey);
					if(old.content().containsKey(partitionId)){
						JsonObject x = (JsonObject) old.content().get(partitionId);
						long fromOffset = x.getLong(Constants.FROMOFFSET);

						old.content()
						.put(partitionId, 
								JsonObject.empty().
									put(Constants.FROMOFFSET, fromOffset).
									put(Constants.UNTILOFFSET, offset.untilOffset()));
					}else{
						old.content()
						.put(partitionId, 
								JsonObject.empty().
									put(Constants.FROMOFFSET, offset.fromOffset()).
									put(Constants.UNTILOFFSET, offset.untilOffset()));
					}
					return bucket.async().replace(old);
				}else{
					return bucket.async().insert(JsonDocument.create(offsetKey, JsonObject.empty()
							.put(String.valueOf(offset.partition()), 
										JsonObject.empty().
											put(Constants.FROMOFFSET, offset.fromOffset()).
											put(Constants.UNTILOFFSET, offset.untilOffset()))));
				}
			}
			
		})
		.retryWhen(
				  RetryBuilder
				    .anyOf(CASMismatchException.class, DocumentAlreadyExistsException.class)
				    .max(5)
				    .delay(Delay.linear(TimeUnit.MILLISECONDS, 2000, 100, 2))
				  .build()
				)
		.toList()
		.toBlocking()
	    .singleOrDefault(null);
	}
	@Override
	public void upsert(Iterator<Tuple2<String, UsableColumns>> actual, OffsetRange offset) {
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		final Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		List<String> queries = new ArrayList<>();
		final Map<String, JsonDocument> map = new HashMap<>();
		final Set<Long> fullLeagues = new HashSet<>();
		while(actual.hasNext()){
			Tuple2<String, UsableColumns> qry = actual.next();
			String key = new StringBuffer()
					.append(this.topic).append(":")
					.append(Constants.AGG_HEADER).append(":")
					.append(qry._1)
					.toString();
			
			queries.add(key);
			map.put(key, JsonDocument.create(key, 
					JsonObject.empty().put(Constants.COUNT, qry._2.getUsers())
					.put(Constants.SUM, qry._2.getAmount())));
			if(qry._2.getFullLeagues()!=null)
				fullLeagues.addAll(qry._2.getFullLeagues());
		}
		/* TO DO
		 * make it atomic
		 */
		if(offset!=null){
			if(!map.isEmpty()){
				addAggregates(bucket, queries, map);
			}
			if(!fullLeagues.isEmpty()){
				addFullLeagues(bucket, fullLeagues);
			}
			updatePartitions(bucket, offset);
		}
		cluster.disconnect();
	}

	@Override
	public void upsert(OffsetRange[] offsets) {
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		JsonObject json = JsonObject.empty();
		for(OffsetRange ofs : offsets){
			json.put(String.valueOf(ofs.partition()), JsonObject.empty().
					put(Constants.FROMOFFSET, ofs.fromOffset()).
					put(Constants.UNTILOFFSET, ofs.untilOffset()));
		}
		if(!bucket.exists(this.topic+":"+Constants.PARTITION_KEY)){
			bucket.upsert(JsonDocument.create(this.topic+":"+Constants.PARTITION_KEY, JsonObject.empty()
					.put(this.topic, json)));
		}else{
			bucket.mutateIn(this.topic+":"+Constants.PARTITION_KEY)
				.upsert(this.topic, json);
		}
		cluster.disconnect();
	}
}
