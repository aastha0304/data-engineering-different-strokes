package spark_aggregator.round.sparkoperations;

import java.io.Serializable;
import java.util.Date;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark_aggregator.round.RoundMeta;
import spark_aggregator.round.utils.Constants;
import spark_aggregator.round.utils.Helpers;

public class RoundIdMapper implements PairFunction<ConsumerRecord<Long,GenericRecord>, 
Long, RoundMeta>, Serializable {
	/** Commented part is for working with schema taht has changed over time, uncomment if this case comes up 
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private void fillMeta(RoundMeta meta, GenericRecord record){
		meta.setActive((new Date((long)record.get(Constants.ROUNDLOCKTIMEDB_KEY))).after(new Date()));
		meta.setCountry(Long.parseLong(record.get(Constants.COUNTRYID_KEY).toString()));
		meta.setGame((long)((int)record.get(Constants.GAMEID_KEY)));

		meta.setOpenTime(Helpers.getDays((long)record.get(Constants.ROUNDLOCKTIMEDB_KEY), 3));
		meta.setRoundStartTime((long)record.get(Constants.ROUNDLOCKTIMEDB_KEY));

		meta.setHalfTime( Helpers.calcHalfTime ( meta.getOpenTime(), meta.getRoundStartTime() ));
		meta.setId((long)record.get(Constants.ROUNDID_KEY));
		
		//HACK, only till ipl
		
		meta.setTourFormat((int)record.get(Constants.TOURFORMAT_KEY));
		meta.setTourMatch((int)record.get(Constants.TOURMATCH_KEY));
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Tuple2<Long, RoundMeta> call(ConsumerRecord<Long, GenericRecord> arg0) throws Exception {	
		GenericRecord record = arg0.value(); 
		RoundMeta roundMeta = new RoundMeta();
		fillMeta(roundMeta, record);
		return new Tuple2(record.get(Constants.ROUNDID_KEY), roundMeta);
	}
}
