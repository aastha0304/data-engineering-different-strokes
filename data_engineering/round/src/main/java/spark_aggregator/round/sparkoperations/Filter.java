package spark_aggregator.round.sparkoperations;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import spark_aggregator.round.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

public class Filter implements Function<ConsumerRecord<Long, GenericRecord>, Boolean>, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    long gameId;
	public Filter(long gameId) {
		this.gameId = gameId;
	}

	@Override
	public Boolean call(ConsumerRecord<Long, GenericRecord> v1) throws Exception {
		// TODO Auto-generated method stub
		long now = System.currentTimeMillis();
		GenericRecord record = v1.value();
		//if(record.get(Constants.OPENTIMEDB_KEY)==null)
			//return false;
		if((long)((int)record.get(Constants.GAMEID_KEY))!=this.gameId)
			return false;
		if((long)record.get(Constants.ROUNDLOCKTIMEDB_KEY) < now)
			return false;
		return true;
	}
	
}
