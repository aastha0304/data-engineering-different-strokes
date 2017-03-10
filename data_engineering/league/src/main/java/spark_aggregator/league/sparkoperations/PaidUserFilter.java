package spark_aggregator.league.sparkoperations;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

import spark_aggregator.league.utils.Constants;

public class PaidUserFilter implements Function<ConsumerRecord<Long, GenericRecord>, Boolean>, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(ConsumerRecord<Long, GenericRecord> arg0) throws Exception {
		// TODO Auto-generated method stub
		GenericRecord record = arg0.value();
		if((double)record.get(Constants.ENTRYFEE_KEY)!=0 && arg0.key().equals(record.get(Constants.LEAGUEID_KEY)))
			return true;
		return false;
	}
}
