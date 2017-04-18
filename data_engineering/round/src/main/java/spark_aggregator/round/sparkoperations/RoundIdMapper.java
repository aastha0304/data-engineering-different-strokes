package spark_aggregator.round.sparkoperations;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark_aggregator.round.RoundMeta;
import spark_aggregator.round.utils.Constants;
import spark_aggregator.round.utils.Helpers;
import spark_aggregator.round.utils.SchemaHandler;

public class RoundIdMapper implements PairFunction<ConsumerRecord<Long,GenericRecord>, 
Long, RoundMeta>, Serializable {
	/** Commented part is for working with schema taht has changed over time, uncomment if this case comes up 
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String schemaString;
	
	@SuppressWarnings("unused")
	private GenericRecord getRecordInLatestSchema(GenericRecord record) throws IOException{
		return SchemaHandler.convertRecordSchema(record, this.schemaString);
	}
	
	private void fillMeta(RoundMeta meta, GenericRecord record){
		meta.setActive((new Date((long)record.get(Constants.ROUNDLOCKTIMEDB_KEY))).after(new Date()));
		meta.setCountry(Constants.INDIA_KEY);
		meta.setGame(Constants.CRICKET_KEY);
		meta.setHalfTime( Helpers.calcHalfTime ( (long) record.get(Constants.OPENTIMEDB_KEY), (long) record.get(Constants.ROUNDLOCKTIMEDB_KEY) ));
		meta.setId((long)record.get(Constants.ROUNDID_KEY));
		meta.setOpenTime((long)record.get(Constants.OPENTIMEDB_KEY));
		meta.setRoundStartTime((long)record.get(Constants.ROUNDLOCKTIMEDB_KEY));
		meta.setTourFormat((int)record.get(Constants.TOURFORMAT_KEY));
		meta.setTourMatch((int)record.get(Constants.TOURMATCH_KEY));
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Tuple2<Long, RoundMeta> call(ConsumerRecord<Long, GenericRecord> arg0) throws Exception {	
		//Uncomment this line for evolving schema
		//GenericRecord record = getRecordInLatestSchema(arg0.value());
		GenericRecord record = arg0.value(); 
		RoundMeta roundMeta = new RoundMeta();
		fillMeta(roundMeta, record);
		//hack, actual value should be put in clusterid_key, but commented schema evolution for now
//		if(p2c.containsKey(record.get(Constants.PRODUCTID_KEY)))
//			roundMeta.setClusterId(p2c.get(record.get(Constants.PRODUCTID_KEY)));
//		else
//			roundMeta.setClusterId(0);
		return new Tuple2(record.get(Constants.ROUNDID_KEY), roundMeta);
	}
}
