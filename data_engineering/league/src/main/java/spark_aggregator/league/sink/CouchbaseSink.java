package spark_aggregator.league.sink;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.DocumentFragment;

import scala.Tuple2;
import spark_aggregator.league.UsableColumns;
import spark_aggregator.league.utils.Constants;

public class CouchbaseSink implements Sink, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Properties properties;
	public CouchbaseSink(Properties properties){
		this.properties = properties;
	}
	
	@Override
	public Map<TopicPartition, Long> getAndUpdateOffsets() {
		// TODO Auto-generated method stub 
		Map<TopicPartition, Long> res = new HashMap<>();
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		if(bucket.exists(Constants.LEAGUETOPIC+":"+Constants.PARTITION_KEY)){
			DocumentFragment<Lookup> json = bucket.lookupIn(Constants.LEAGUETOPIC+":"+Constants.PARTITION_KEY)
					.get(Constants.LEAGUETOPIC).execute();
			JsonObject x = json.content(0, JsonObject.class);
			if(x!=null){
				Map<String, Object> m = x.toMap();
				//HashMap<String, HashMap<String, Long>> partitions = json.content(0)
				Set<Entry<String, Object>> entries = m.entrySet();
				for(Entry<String, Object> entry:entries){
					@SuppressWarnings("unchecked")
					HashMap<String, Integer> v = (HashMap<String, Integer>) entry.getValue();
					res.put(new TopicPartition(Constants.LEAGUETOPIC, Integer.parseInt(entry.getKey())), 
							v.get(Constants.UNTILOFFSET).longValue());		
				}
			}
		}
		// Disconnect and clear all allocated resources
		cluster.disconnect();
		return res;
	}

	@Override
	public void upsert(Tuple2<Tuple2<Long, Long>, UsableColumns> actual) {
		// TODO Auto-generated method stub
		CouchbaseCluster cluster = CouchbaseCluster.create((String)properties.get("com.couchbase.nodes"));
		Bucket bucket = cluster.openBucket((String)properties.get("com.couchbase.bucket"));
		Set<Long> fullLeagues = actual._2.getFullLeagues();
		long latestRoundCount = actual._2.getUsers();
		double latestRoundSum = actual._2.getAmount();
		long latestRoundClusterCount = actual._2.getUsers();
		double latestRoundClusterSum = actual._2.getAmount();
		System.out.println(actual._1._1+":"+actual._1._2);
		System.out.println("##################ROUND LEVEL###################");
		System.out.println(latestRoundCount);
		System.out.println(latestRoundSum);
		System.out.println("##################ROUND CLUSTER LEVEL###################");
		System.out.println(latestRoundClusterCount);
		System.out.println(latestRoundClusterSum);
		if(bucket.exists(Constants.LEAGUETOPIC+":"+Constants.AGG_HEADER+":"+actual._1._1)){
			JsonDocument roundAgg = bucket.get(Constants.LEAGUETOPIC+":"+Constants.AGG_HEADER+":"+actual._1._1);
//			latestRoundCount += (int) roundAgg.content().get(Constants.COUNT);
			latestRoundSum += (double) roundAgg.content().get(Constants.SUM);
			bucket
		    .mutateIn(Constants.LEAGUETOPIC+":"+Constants.AGG_HEADER+":"+actual._1._1)
		    .counter(Constants.COUNT, (long) latestRoundCount)
		    .replace(Constants.SUM, latestRoundSum)
		    //.withCas(1234L)
		    .execute();
		}else{
			bucket.insert(JsonDocument.create(Constants.LEAGUETOPIC+":"+Constants.AGG_HEADER+":"+actual._1._1,
					JsonObject.empty()
						.put(Constants.COUNT, latestRoundCount)
						.put(Constants.SUM, latestRoundSum)));
		}

		if(bucket.exists(Constants.LEAGUETOPIC+":"+Constants.AGG_HEADER+":"+actual._1._1+":"+actual._1._2)){
			JsonDocument roundClusterAgg = bucket.get(Constants.LEAGUETOPIC+":"+Constants.AGG_HEADER+":"
					+actual._1._1+":"+actual._1._2);
//			latestRoundClusterCount += (int) roundClusterAgg.content().get(Constants.COUNT);
			latestRoundClusterSum += (double) roundClusterAgg.content().get(Constants.SUM);
			bucket
		    .mutateIn(Constants.LEAGUETOPIC+":"+Constants.AGG_HEADER+":"+actual._1._1+":"+actual._1._2)
		    .counter(Constants.COUNT, (long) latestRoundClusterCount)
		    .replace(Constants.SUM, latestRoundClusterSum)
		    //.withCas(1234L)
		    .execute();
		}else{
			bucket.insert(JsonDocument.create(Constants.LEAGUETOPIC+":"+Constants.AGG_HEADER+":"+actual._1._1+":"+actual._1._2,
					JsonObject.empty()
						.put(Constants.COUNT, latestRoundClusterCount)
						.put(Constants.SUM, latestRoundClusterSum)));
		}
		if(!fullLeagues.isEmpty()){
			if(!bucket.exists(Constants.LEAGUETOPIC+":"+Constants.FULLLEAGUES_KEY))
				bucket.insert(JsonArrayDocument.create(Constants.LEAGUETOPIC+":"+Constants.FULLLEAGUES_KEY, 
						JsonArray.from(fullLeagues)));
			else	
				for(Long leagueId: fullLeagues)
					bucket.listAppend(Constants.LEAGUETOPIC+":"+Constants.FULLLEAGUES_KEY, leagueId);
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
		if(!bucket.exists(Constants.LEAGUETOPIC+":"+Constants.PARTITION_KEY)){
			bucket.upsert(JsonDocument.create(Constants.LEAGUETOPIC+":"+Constants.PARTITION_KEY, JsonObject.empty()
					.put(Constants.LEAGUETOPIC, json)));
		}else{
			bucket.mutateIn(Constants.LEAGUETOPIC+":"+Constants.PARTITION_KEY)
				.upsert(Constants.LEAGUETOPIC, json);
		}
		cluster.disconnect();
	}
}
