package spark_aggregator.round.sink;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.codehaus.jackson.map.ObjectMapper;

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
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import scala.Tuple2;
import spark_aggregator.round.RoundMeta;
import spark_aggregator.round.utils.Constants;
import spark_aggregator.round.utils.Helpers;

class Aggregation implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	long count;
	double sum;
	double maxCount;
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public double getSum() {
		return sum;
	}
	public void setSum(double sum) {
		this.sum = sum;
	}
	public double getMaxCount() {
		return maxCount;
	}
	public void setMaxCount(double d) {
		this.maxCount = d;
	}
}
public class CouchbaseSink implements Sink, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Properties properties;
	private String ROUNDTOPIC;
	private String LEAGUETOPIC;
	final String offsetKey ;
	final String allRoundsKey;
	final String halfTimeRoundKey;
	final String roundAggKey;

	public CouchbaseSink(Properties properties){
		this.properties = properties;
		this.ROUNDTOPIC = (String)properties.get("round.topic");
		this.LEAGUETOPIC = (String)properties.get("league.topic");
		allRoundsKey = new StringBuilder()
				.append(ROUNDTOPIC)
				.append(":")
				.append(Constants.ALLROUNDS_KEY).toString();
		offsetKey = new StringBuilder()
				.append(ROUNDTOPIC)
				.append(":")
				.append(Constants.PARTITION_KEY).toString();
		halfTimeRoundKey = new StringBuilder()
				.append(ROUNDTOPIC)
				.append(":")
				.append(Constants.HALFTIME_KEY)
				.append(":").toString();
		roundAggKey = new StringBuilder()
				.append(LEAGUETOPIC).append(":")
				.append(Constants.AGG_HEADER).append(":").toString();
	}
	
	@Override
	public Map<TopicPartition, Long> getAndUpdateOffsets() {
		Map<TopicPartition, Long> res = new HashMap<>();
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.round.bucket"));
		if(bucket.exists(offsetKey)){
			JsonDocument json = bucket.get(offsetKey);
			JsonObject x = json.content();
			if(x!=null){
				Map<String, Object> m = x.toMap();
				Set<Entry<String, Object>> entries = m.entrySet();
				for(Entry<String, Object> entry:entries){
					@SuppressWarnings("unchecked")
					HashMap<String, Integer> v = (HashMap<String, Integer>) entry.getValue();
					res.put(new TopicPartition(ROUNDTOPIC, Integer.parseInt(entry.getKey())), 
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
	private void addRoundMeta(final Bucket bucket, List<String> roundMetaQueries, final Map<String, JsonDocument> map){
		Observable.from(roundMetaQueries)
		        .flatMap(new Func1<String, Observable<JsonDocument>>() {
		            @Override
		            public Observable<JsonDocument> call(String id) {
		            	return bucket.async().upsert(map.get(id));
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
	private boolean isRoundActive(JsonDocument round, long current) {
		if(round.content().getLong(Constants.OPENTIME_KEY) <= current
				&& round.content().getLong(Constants.ROUNDLOCKTIME_KEY) >= current) {
			return true;
		}
		return false;
	}
	
	private boolean isRoundToUpdate(JsonDocument round, long current){
		// don't run half time calc for too close half-times

		if( round.content().getLong(Constants.ROUNDLOCKTIME_KEY) -
				round.content().getLong(Constants.HALFTIME_KEY) < 10) 
			return false;
		
		if(isRoundActive(round, current) && round.content().getLong(Constants.HALFTIME_KEY) <= current) 
			return true;
		return false;
	}
	private JsonArray getJsonArrayBucketIds(){
		//this is  hardcoded - change to take from couchbase
		return JsonArray.empty().add("0").add("1").add("2")
				.add("3").add("4").add("5")
				.add("6").add("7").add("8")
				.add("9").add("10").add("11")
				.add("12").add("13").add("14")
				.add("15").add("16").add("17");
	}
	@SuppressWarnings("unchecked")
	private void doHalfTimeCalc(final Bucket leagueBucket, final Bucket bucket){
		System.out.println("in halftime function");
		final long current = new Date().getTime();
		final Set<Tuple2<String, String>> otherActiveRoundsClusters = new HashSet<>();
		final Set<String> otherActiveRounds = new HashSet<>();
		final Set<Tuple2<String, String>> roundsClustersForHalfTime = new HashSet<>();
		final Set<String> roundsForHalfTime = new HashSet<>();
		final JsonArray bucketIds = getJsonArrayBucketIds();
		if(bucket.exists(allRoundsKey)){
			List<Object> rounds = bucket.get(allRoundsKey, JsonArrayDocument.class).content().toList();
			Observable
			.from(rounds)
			.forEach(new Action1<Object>() {
				@Override
				public void call(Object arg0) {
					String id = arg0.toString();
					String roundMetaLookupId = new StringBuilder(halfTimeRoundKey).append(id).toString();
					JsonDocument round = bucket.get(roundMetaLookupId);
					
					if(isRoundToUpdate(round, current)){
						System.out.println("Round "+id+" is to be updated");
						for(Object bucketId: bucketIds){
							String bucketid = bucketId.toString();
//							String lookupId = new StringBuilder(roundAggKey)
//									.append(id)
//									.append(":")
//									.append(bucketid).toString();
							//add anyways, needed for "o_ ..." kind of aggregations
//							if(bucket.exists(lookupId)){
//								roundsClustersForHalfTime.add(new Tuple2<String, String>(id, bucketid));
//							}
							roundsClustersForHalfTime.add(new Tuple2<String, String>(id, bucketid));
						}
						roundsForHalfTime.add(roundMetaLookupId);
					}
					if(wasRoundActive(round, current)){
						System.out.println("Round "+id+" is to active");

						for(Object bucketId: bucketIds){
							String bucketid = bucketId.toString();
							
							// add anyways, needed for "o_ ..." kind of aggregations
//							if(bucket.exists(lookupId)){
//								otherActiveRoundsClusters.add(new Tuple2<String, String>(lookupId, bucketid));
//							}
							otherActiveRoundsClusters.add(new Tuple2<String, String>(id, bucketid));
						}
						otherActiveRounds.add(id);
					}
					System.out.println("Round "+id+" checked for updation and activity");
				}
			});
			
			Map<String,Aggregation> accumulation = null;
			if(!otherActiveRoundsClusters.isEmpty()){
				accumulation = Observable
				.from(otherActiveRoundsClusters)
				.flatMap(new Func1<Tuple2<String, String>, Observable<Map<String, Aggregation>>>() {
		            @Override
		            public Observable<Map<String,Aggregation>> call(Tuple2<String, String> lookupId) {
		            	String leagueLookupId = new StringBuilder(roundAggKey)
								.append(lookupId._1)
								.append(":")
								.append(lookupId._2).toString();
						JsonDocument roundCluster = leagueBucket.get(leagueLookupId);
						
						String roundMetaLookupId = new StringBuilder(halfTimeRoundKey).append(lookupId._1).toString();
						JsonDocument round = bucket.get(roundMetaLookupId);
						
						Map<String,Aggregation> bucketBased = new HashMap<>();
						Aggregation aggregation = new Aggregation();
						if(roundCluster!=null){
							aggregation.setCount(roundCluster.content().getLong(Constants.COUNT));
							aggregation.setMaxCount(roundCluster.content().getLong(Constants.COUNT)/Helpers.f(round.content()));
							//aggregation.setMaxCount(roundCluster.content().getLong(Constants.COUNT));
							aggregation.setSum(roundCluster.content().getDouble(Constants.SUM));
						}else{
							aggregation.setCount(0);
							aggregation.setMaxCount(0);
							aggregation.setSum(0);
						}
						bucketBased.put(lookupId._2, aggregation);
			            return Observable.just(bucketBased);
		            }
		        })
				.scan(new Func2<Map<String, Aggregation>,Map<String, Aggregation>,Map<String, Aggregation>>() {
					@Override
					public Map<String, Aggregation> call(Map<String, Aggregation> summations, Map<String, Aggregation> arg0) {
						Entry<String, Aggregation> entry = arg0.entrySet().iterator().next();
						String key = entry.getKey();
						Aggregation arg = entry.getValue();
						if(summations.containsKey(entry.getKey())){
							Aggregation agg = new Aggregation();
							agg.setCount(arg.getCount()+summations.get(key).getCount());
							agg.setMaxCount(arg.getMaxCount()>summations.get(key).getMaxCount()?
									arg.getMaxCount():summations.get(key).getMaxCount());
							agg.setSum(arg.getSum()+summations.get(key).getSum());
							summations.put(key, agg);	
						}
						else
							summations.put(key, arg);
						return summations;
					}
			    })
				.last()
				.toBlocking()
			    .singleOrDefault(null);
			}
			display(otherActiveRoundsClusters);
			display(roundsClustersForHalfTime);
			final Map<String,Aggregation> accumulationCopy = accumulation;

			if(!roundsClustersForHalfTime.isEmpty()){
				//For calculation of current round
				Observable
				.from(roundsClustersForHalfTime)
				.flatMap(new Func1<Tuple2<String, String>, Observable<JsonDocument>>() {
					private JsonObject fillHalfTime(double sum, long count, double other_sum, long other_count, double max_count){
						return JsonObject.create()
								.put(Constants.SUM, sum)
								.put(Constants.COUNT, count)
								.put(Constants.OTHERSUM, other_sum)
								.put(Constants.OTHERCOUNT, other_count)
								.put(Constants.HALFTIMENESTED_KEY, current)
								.put(Constants.OTHER_ACTIVE_R_COUNT, otherActiveRounds.size()-1)
								.put(Constants.MAX_AMT, max_count);
					}
		            @Override
		            public Observable<JsonDocument> call(Tuple2<String, String> id) {
						//round data gotten from league event aggregation
						String leagueJoinLookupId = new StringBuilder(roundAggKey)
								.append(id._1)
								.append(":")
								.append(id._2).toString();
						String roundMetaLookupId = new StringBuilder(halfTimeRoundKey)
								.append(id._1)
								.toString();
						JsonDocument roundFromRoundMeta = bucket.get(roundMetaLookupId);
						JsonObject thisHalfTime;
						
						if(leagueBucket.exists(leagueJoinLookupId)){
							JsonDocument roundFromLeagueJoin = leagueBucket.get(leagueJoinLookupId);

							if(accumulationCopy!=null)
								thisHalfTime = fillHalfTime(roundFromLeagueJoin.content().getDouble(Constants.SUM),
										roundFromLeagueJoin.content().getLong(Constants.COUNT),
										accumulationCopy.get(id._2).getSum()-roundFromLeagueJoin.content().getDouble(Constants.SUM),
										accumulationCopy.get(id._2).getCount()-roundFromLeagueJoin.content().getLong(Constants.COUNT),
										Helpers.div(accumulationCopy.get(id._2).getMaxCount(), accumulationCopy.get(id._2).getCount()));
							else
								thisHalfTime = fillHalfTime(roundFromLeagueJoin.content().getDouble(Constants.SUM),
										roundFromLeagueJoin.content().getLong(Constants.COUNT),
										0.0,0L,0L);
							
						}else{
							if(accumulationCopy!=null)
								thisHalfTime = fillHalfTime(0.0,0L,accumulationCopy.get(id._2).getSum(),
										accumulationCopy.get(id._2).getCount(), 
										Helpers.div(accumulationCopy.get(id._2).getMaxCount(), accumulationCopy.get(id._2).getCount()));

							else
								thisHalfTime = fillHalfTime(0.0,0L,0.0,0L,0L);
						}
						String roundClusterMetaLookupId = new StringBuilder(halfTimeRoundKey)
								.append(id._1)
								.append(":")
								.append(id._2)
								.toString();
						if(bucket.exists(roundClusterMetaLookupId)){
							JsonDocument roundClusterData = bucket.get(roundClusterMetaLookupId);
							roundClusterData.content().put(Constants.HALFTIME_KEY, 
									Helpers.calcHalfTime(roundClusterData.content().getLong(Constants.HALFTIME_KEY), 
											roundFromRoundMeta.content().getLong(Constants.ROUNDLOCKTIME_KEY)));
							if(roundClusterData.content().containsKey(Constants.TIMEBASEDAGG_KEY)){
								JsonObject old = (JsonObject) roundClusterData.content().get(Constants.TIMEBASEDAGG_KEY);
								if(!old.containsKey(String.valueOf(current)))
									old.put(String.valueOf(current), thisHalfTime);
								
								roundClusterData.content().put(Constants.TIMEBASEDAGG_KEY, old);
								
							}else
								roundClusterData.content().put(Constants.TIMEBASEDAGG_KEY, JsonObject.create()
										.put(String.valueOf(current), thisHalfTime));
							return bucket.async().replace(roundClusterData);
						}else{
							JsonDocument roundClusterData = JsonDocument.create(roundClusterMetaLookupId, 
									roundFromRoundMeta.content());
							roundClusterData.content().put(Constants.HALFTIME_KEY, 
									Helpers.calcHalfTime(roundFromRoundMeta.content().getLong(Constants.HALFTIME_KEY), 
											roundFromRoundMeta.content().getLong(Constants.ROUNDLOCKTIME_KEY)));
							roundClusterData.content().put(Constants.TIMEBASEDAGG_KEY, JsonObject.create()
									.put(String.valueOf(current), thisHalfTime));
							return bucket.async().insert(roundClusterData);
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
				
				if(!roundsForHalfTime.isEmpty()){
					Observable.from(roundsForHalfTime)
					.flatMap(new Func1<String, Observable<JsonDocument>>() {

						@Override
						public Observable<JsonDocument> call(String roundMetaLookupId) {
							// TODO Auto-generated method stub
							JsonDocument roundFromRoundMeta = bucket.get(roundMetaLookupId);
							roundFromRoundMeta.content().put(Constants.HALFTIME_KEY, Helpers.calcHalfTime(roundFromRoundMeta.content().getLong(Constants.HALFTIME_KEY), 
											roundFromRoundMeta.content().getLong(Constants.ROUNDLOCKTIME_KEY)));
							return bucket.async().replace(roundFromRoundMeta);
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
			}
		}
	}
	private void display(Set<Tuple2<String, String>> otherActiveRoundsClusters) {
		if(!otherActiveRoundsClusters.isEmpty())
			for(Tuple2<String, String> roundBucket: otherActiveRoundsClusters)
				System.out.println(roundBucket._1+" "+roundBucket._2);
		else
			System.out.println("roundcluster is empty!!!!");
	}

	@SuppressWarnings("unchecked")
	private void addNewRounds(final Bucket bucket, final Set<Long> newRounds){
		Observable
				.just(allRoundsKey)
				.retryWhen(
						  RetryBuilder
						    .anyOf(CASMismatchException.class, DocumentAlreadyExistsException.class)
						    .max(5)
						    .delay(Delay.linear(TimeUnit.MILLISECONDS, 2000, 100, 2))
						  .build()
						)
				.forEach(new Action1<String>(){
					@Override
					public void call(String id) {
						if(!bucket.exists(id)){
							bucket.insert(JsonArrayDocument.create(id, JsonArray.empty()));
						}
						for(long newRound: newRounds){
							bucket.setAdd(id, newRound);
						}
					}
				})
				;
	}
	@Override
	@SuppressWarnings("unchecked")
	public void upsert(Iterator<Tuple2<Long, RoundMeta>> rows, OffsetRange offset) {
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		final Bucket leagueBucket = cluster.openBucket((String)properties.get("com.couchbase.league.bucket"));
		final Bucket roundBucket = cluster.openBucket((String)properties.get("com.couchbase.round.bucket"));
		List<String> queries = new ArrayList<>();
		final Map<String, JsonDocument> map = new HashMap<>();
		final Set<Long> newRounds = new HashSet<>();
		while(rows.hasNext()){
			Tuple2<Long, RoundMeta> row = rows.next();
			String roundKey = row._1.toString();
			queries.add(roundKey);
			newRounds.add(row._1);
			ObjectMapper om = new ObjectMapper();
			Map<String,Object> metaMap = om.convertValue(row._2,Map.class);
			map.put(roundKey, JsonDocument.create(new StringBuilder(halfTimeRoundKey).append(roundKey).toString(),
					JsonObject.from(metaMap)));
		}
		if(!map.isEmpty()){
			addRoundMeta(roundBucket, queries, map);
			addNewRounds(roundBucket, newRounds);
		}
		doHalfTimeCalc(leagueBucket, roundBucket);
		if(offset!=null){
			updatePartitions(roundBucket, offset);
		}
		cluster.disconnect();
	}

	@Override
	public void upsert(OffsetRange[] offsets) {
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		JsonObject json = JsonObject.empty();
		for(OffsetRange ofs : offsets)
			json.put(String.valueOf(ofs.partition()), JsonObject.empty().
					put(Constants.FROMOFFSET, ofs.fromOffset()).
					put(Constants.UNTILOFFSET, ofs.untilOffset()));
		
		if(!bucket.exists(ROUNDTOPIC+":"+Constants.PARTITION_KEY))
			bucket.upsert(JsonDocument.create(ROUNDTOPIC+":"+Constants.PARTITION_KEY, JsonObject.empty()
					.put(ROUNDTOPIC, json)));
		else
			bucket.mutateIn(ROUNDTOPIC+":"+Constants.PARTITION_KEY)
				.upsert(ROUNDTOPIC, json);
		
		cluster.disconnect();
	}
}
