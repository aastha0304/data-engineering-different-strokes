package spark_aggregator.league.sparkoperations;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark_aggregator.league.utils.Constants;
import spark_aggregator.league.utils.SchemaHandler;
public class LeagueUserIndexer implements PairFunction<ConsumerRecord<Long,GenericRecord>, 
Tuple2<Long,Long>, GenericRecord>, Serializable {
	/** Commented part is for working with schema taht has changed over time, uncomment if this case comes up 
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HashMap<Long, Long> p2c;
	private String schemaString;
	
	public LeagueUserIndexer(HashMap<Long, Long> p2c){
		this.p2c = p2c;
		//this.schemaString = schema._2;
	}
	@SuppressWarnings("unused")
	private GenericRecord getRecordInLatestSchema(GenericRecord record) throws IOException{
		return SchemaHandler.convertRecordSchema(record, this.schemaString);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Tuple2<Tuple2<Long, Long>, GenericRecord> call(ConsumerRecord<Long, GenericRecord> arg0) throws Exception {	
		//Uncomment this line for evolving schema
		//GenericRecord record = getRecordInLatestSchema(arg0.value());
		GenericRecord record = arg0.value(); 
		if(p2c.containsKey(record.get(Constants.PRODUCTID_KEY)))
			record.put(Constants.PRODUCTID_KEY, p2c.get(record.get(Constants.PRODUCTID_KEY)));
		else
			record.put(Constants.PRODUCTID_KEY, 0);
		return new Tuple2(new Tuple2(record.get(Constants.LEAGUEID_KEY),record.get(Constants.USERID_KEY)), record);
	}
}
