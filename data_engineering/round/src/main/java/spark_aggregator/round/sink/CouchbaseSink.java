package spark_aggregator.round.sink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.codehaus.jackson.map.ObjectMapper;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.util.retry.RetryBuilder;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.functions.Func2;
import scala.Tuple2;
import spark_aggregator.round.RoundMeta;
import spark_aggregator.round.utils.Constants;
public class CouchbaseSink implements Sink, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Properties properties;
	final String offsetKey = new StringBuffer()
			.append(Constants.ROUNDTOPIC)
			.append(":")
			.append(Constants.PARTITION_KEY).toString();
	final String allRoundsKey = new StringBuffer()
			.append(Constants.ROUNDTOPIC)
			.append(":")
			.append(Constants.ALLROUNDS_KEY).toString();
	
	public CouchbaseSink(Properties properties){
		this.properties = properties;
	}
	
	@Override
	public Map<TopicPartition, Long> getAndUpdateOffsets() {
		// TODO Auto-generated method stub 
		Map<TopicPartition, Long> res = new HashMap<>();
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		if(bucket.exists(Constants.ROUNDTOPIC+":"+Constants.PARTITION_KEY)){
			DocumentFragment<Lookup> json = bucket.lookupIn(Constants.ROUNDTOPIC+":"+Constants.PARTITION_KEY)
					.get(Constants.ROUNDTOPIC).execute();
			JsonObject x = json.content(0, JsonObject.class);
			if(x!=null){
				Map<String, Object> m = x.toMap();
				//HashMap<String, HashMap<String, Long>> partitions = json.content(0)
				Set<Entry<String, Object>> entries = m.entrySet();
				for(Entry<String, Object> entry:entries){
					@SuppressWarnings("unchecked")
					HashMap<String, Integer> v = (HashMap<String, Integer>) entry.getValue();
					res.put(new TopicPartition(Constants.ROUNDTOPIC, Integer.parseInt(entry.getKey())), 
							v.get(Constants.UNTILOFFSET).longValue());		
				}
			}
		}
		// Disconnect and clear all allocated resources
		cluster.disconnect();
		return res;
	}

	@SuppressWarnings("unchecked")
	private void updatePartitions(final Bucket bucket, final OffsetRange offset){
		final JsonObject json = JsonObject.empty();
		json.put(String.valueOf(offset.partition()), JsonObject.empty().
					put(Constants.FROMOFFSET, offset.fromOffset()).
					put(Constants.UNTILOFFSET, offset.untilOffset()));
		Observable
		.just(offsetKey)
		.flatMap(new Func1<String, Observable<JsonDocument>>(){

			@Override
			public Observable<JsonDocument> call(String id) {
				// TODO Auto-generated method stub
				if(bucket.exists(id)){
					return bucket.async().replace(JsonDocument.create(offsetKey, JsonObject.empty()
							.put(Constants.ROUNDTOPIC, json)));
				}else{
					return bucket.async().insert(JsonDocument.create(offsetKey, JsonObject.empty()
							.put(Constants.ROUNDTOPIC, json)));
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
		.last()
		.toBlocking()
	    .singleOrDefault(null);
	}
	
	private long getHalfTime(Date startTime, Date endTime){
		long halfTime = (endTime.getTime() - startTime.getTime()) / 2;
		return halfTime;
	}
	
	@SuppressWarnings("unchecked")
	private void addAggregates(final Bucket bucket, List<String> queries, final Map<String, JsonDocument> map){
		Observable.from(queries)
		        .flatMap(new Func1<String, Observable<JsonDocument>>() {
		            @Override
		            public Observable<JsonDocument> call(String id) {
			            	return bucket.async().insert(map.get(id));
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
	private boolean isRoundActive(JsonDocument round, long current){
		if(round.content().getLong(Constants.ROUNDLOCKTIME_KEY)>current)
			return true;
		return false;
	}
	
	private boolean isRoundToUpdate(JsonDocument round, long current){
		//half should not be too close to close time of this will never end
		//and;
		//current and half time should be close by
		if((Math.abs(round.content().getLong(Constants.ROUNDLOCKTIME_KEY)-
				round.content().getLong(Constants.HALFTIME_KEY)) < 10) &&
				(Math.abs(current-
						round.content().getLong(Constants.HALFTIME_KEY)) < 10)	)
			return true;
		return false;
	}
	@SuppressWarnings("unchecked")
	private void doHalfTimeCalc(final Bucket bucket){
		final long current = new Date().getTime();
		final List<String> roundsForLJ = new LinkedList<>();
		final List<String> roundsForHalfTime = new LinkedList<>();
		if(bucket.exists(allRoundsKey)){
			JsonArrayDocument rounds = bucket.get(allRoundsKey, JsonArrayDocument.class);
			Observable
			.from(rounds.content().toList())
			.subscribe(new Subscriber<Object>(){

				@Override
				public void onCompleted() {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void onError(Throwable arg0) {
					// TODO Auto-generated method stub
					System.err.println("Whoops: " + arg0.getMessage());
				}

				@Override
				public void onNext(Object arg0) {
					// TODO Auto-generated method stub
					String id = arg0.toString();
					//round from round meta
					JsonDocument round = bucket.get(id);
					if(isRoundToUpdate(round, current)){
						roundsForHalfTime.add(id);
					}
					if(isRoundActive(round, current)){
						roundsForLJ.add(id);
					}
				}
				
			});
			//For calculations of other active rounds
			final Tuple2<Long, Double> accumulation = Observable
			.from(roundsForLJ)
			.flatMap(new Func1<String, Observable<Tuple2<Long, Double>>>() {
	            @Override
	            public Observable<Tuple2<Long, Double>> call(String id) {
	            	JsonDocument round = bucket.get(id);
	            	return Observable.just(new Tuple2<>(round.content().getLong(Constants.COUNT),
							round.content().getDouble(Constants.SUM)));
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
			.scan(new Func2<Tuple2<Long,Double>,Tuple2<Long,Double>,Tuple2<Long,Double>>() {
				@Override
				public Tuple2<Long, Double> call(Tuple2<Long,Double> summations, Tuple2<Long, Double> arg0) {
					return new Tuple2<Long, Double>(summations._1+arg0._1, summations._2+arg0._2) ;
				}
		    })
			.last()
			.toBlocking()
		    .singleOrDefault(null);
			//For calculation of current round
			Observable
			.from(roundsForHalfTime)
			.flatMap(new Func1<String, Observable<JsonDocument>>() {
	            @Override
	            public Observable<JsonDocument> call(String id) {
					//round data gotten from league event aggregation
					JsonDocument roundFromLJ = bucket.get(id);
					
					JsonDocument roundFromRM = bucket.get(id);
					JsonObject thisHalfTime = JsonObject.create().put(Constants.SUM, roundFromLJ.content().getDouble(Constants.SUM))
					.put(Constants.COUNT, roundFromLJ.content().getDouble(Constants.COUNT))
					.put(Constants.OTHERCOUNT, accumulation._1-roundFromLJ.content().getDouble(Constants.COUNT))
					.put(Constants.OTHERSUM, accumulation._2-roundFromLJ.content().getDouble(Constants.SUM))
					.put(Constants.HALFTIMENESTED_KEY, current);
					
					roundFromRM.content().put(Constants.HALFTIME_KEY, 
							(roundFromRM.content().getLong(Constants.ROUNDLOCKTIME_KEY)-current)/2);

					if(roundFromRM.content().containsKey(Constants.TIMEBASEDAGG_KEY)){
						Object old = bucket.get(id).content().get(Constants.TIMEBASEDAGG_KEY);
						if(old instanceof JsonArray){
							JsonArray oldArr = (JsonArray)old;
							
							oldArr.add(thisHalfTime);
							roundFromRM.content().put(Constants.TIMEBASEDAGG_KEY, oldArr);
						}
					}else{
						roundFromRM.content().put(Constants.TIMEBASEDAGG_KEY, JsonArray.create().add(thisHalfTime));
					}
					return bucket.async().replace(roundFromRM);
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
	}
	@SuppressWarnings("unchecked")
	private void addNewRounds(final Bucket bucket, final Set<Long> newRounds){
		final JsonArray currentFullLeagues = JsonArray.from(newRounds.toArray());
		Observable
				.just(allRoundsKey)
				.flatMap(new Func1<String, Observable<JsonArrayDocument>>(){

					@Override
					public Observable<JsonArrayDocument> call(String id) {
						// TODO Auto-generated method stub
						if(bucket.exists(id)){
							JsonArrayDocument old = bucket.get(id, JsonArrayDocument.class);
							old.content().add(currentFullLeagues);
							return bucket.async().replace(old);
						}else{
							return bucket.async().insert(JsonArrayDocument.create(allRoundsKey, 
									JsonArray.from(currentFullLeagues)));
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
				.last()
				.toBlocking()
			    .singleOrDefault(null);
	}
	@Override
	@SuppressWarnings("unchecked")
	public void upsert(Iterator<Tuple2<Long, RoundMeta>> rows, OffsetRange offset) {
		// TODO Auto-generated method stub
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		final Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		
		List<String> queries = new ArrayList<>();
		final Map<String, JsonDocument> map = new HashMap<>();
		final Set<Long> newRounds = new HashSet<>();
		while(rows.hasNext()){
			Tuple2<Long, RoundMeta> row = rows.next();
			String roundKey = row._1.toString();
			queries.add(roundKey);
			ObjectMapper om = new ObjectMapper();
			Map<String,Object> metaMap = om.convertValue(row._2,Map.class);
			map.put(roundKey, JsonDocument.create(roundKey, JsonObject.from(metaMap)));
		}
		if(!map.isEmpty()){
			addAggregates(bucket, queries, map);
			addNewRounds(bucket, newRounds);
		}
		doHalfTimeCalc(bucket);
		if(offset!=null){
			updatePartitions(bucket, offset);
		}
		cluster.disconnect();
	}

	@Override
	public void upsert(OffsetRange[] offsets) {
		// TODO Auto-generated method stub
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		JsonObject json = JsonObject.empty();
		for(OffsetRange ofs : offsets){
			json.put(String.valueOf(ofs.partition()), JsonObject.empty().
					put(Constants.FROMOFFSET, ofs.fromOffset()).
					put(Constants.UNTILOFFSET, ofs.untilOffset()));
		}
		if(!bucket.exists(Constants.ROUNDTOPIC+":"+Constants.PARTITION_KEY)){
			bucket.upsert(JsonDocument.create(Constants.ROUNDTOPIC+":"+Constants.PARTITION_KEY, JsonObject.empty()
					.put(Constants.ROUNDTOPIC, json)));
		}else{
			bucket.mutateIn(Constants.ROUNDTOPIC+":"+Constants.PARTITION_KEY)
				.upsert(Constants.ROUNDTOPIC, json);
		}
		cluster.disconnect();
	}
	
}
