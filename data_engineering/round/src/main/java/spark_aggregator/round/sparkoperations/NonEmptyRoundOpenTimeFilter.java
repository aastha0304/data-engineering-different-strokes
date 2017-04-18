package spark_aggregator.round.sparkoperations;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import spark_aggregator.round.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

public class NonEmptyRoundOpenTimeFilter implements Function<ConsumerRecord<Long, GenericRecord>, Boolean>, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(ConsumerRecord<Long, GenericRecord> v1) throws Exception {
		// TODO Auto-generated method stub
		GenericRecord record = v1.value();
		if(record.get(Constants.OPENTIME_KEY)==null)
			return false;
		return true;
	}
	
}
