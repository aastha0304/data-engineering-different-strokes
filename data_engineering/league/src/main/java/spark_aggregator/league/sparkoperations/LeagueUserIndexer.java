package spark_aggregator.league.sparkoperations;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.PairFunction;


import scala.Tuple2;
import spark_aggregator.league.utils.Constants;
import spark_aggregator.league.utils.FeeSizeRanges;
import spark_aggregator.league.utils.SchemaHandler;
public class LeagueUserIndexer implements PairFunction<ConsumerRecord<Long,GenericRecord>, 
Tuple2<Long,Long>, GenericRecord>, Serializable {
	/** Commented part is for working with schema taht has changed over time, uncomment if this case comes up 
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<FeeSizeRanges> p2c;
	private String schemaString;
	
	public LeagueUserIndexer(List<FeeSizeRanges> navigableMap){
		this.p2c = navigableMap;
		//this.schemaString = schema._2;
	}
	@SuppressWarnings("unused")
	private GenericRecord getRecordInLatestSchema(GenericRecord record) throws IOException{
		return SchemaHandler.convertRecordSchema(record, this.schemaString);
	}
	
	int findProduct(float fee, int size){
		int idx = 1;
		for(FeeSizeRanges f: p2c ){
			if(f.getMinFee()<=fee&&f.getMaxFee()>fee&&f.getMinSize()<=size&&f.getMaxSize()>=size)
				return idx;
			idx++;
		}
		
		return 0;
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Tuple2<Tuple2<Long, Long>, GenericRecord> call(ConsumerRecord<Long, GenericRecord> arg0) throws Exception {	
		//Uncomment this line for evolving schema
		//GenericRecord record = getRecordInLatestSchema(arg0.value());
		GenericRecord record = arg0.value(); 
	
		int productId = findProduct((float)record.get(Constants.ENTRYFEE_KEY), (int)record.get(Constants.LEAGUESIZE_KEY));
		if(record.get(Constants.LEAGUETYPE_KEY).toString().equals(Constants.GRANDLEAGUETYPE_VAL))
			productId = Constants.GRANDLEAGUEPID;
		//change to cluster id later
		record.put(Constants.PRODUCTID_KEY, productId);			
		return new Tuple2(new Tuple2(record.get(Constants.LEAGUEID_KEY),record.get(Constants.USERID_KEY)), record);
	}
}
