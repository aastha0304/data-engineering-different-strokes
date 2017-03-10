package spark_aggregator.league.sparkoperations;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function2;

public class LeagueUserDeduplicator implements Function2<GenericRecord, GenericRecord, GenericRecord>, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

//	@Override
//	public Tuple2<Tuple2<Long, Long>, GenericRecord> call(
//			Tuple2<Tuple2<Long, Long>, GenericRecord> arg0,
//			Tuple2<Tuple2<Long, Long>, GenericRecord> arg1) throws Exception {
//		// TODO Auto-generated method stub
//		return arg0;
//	}

	@Override
	public GenericRecord call(GenericRecord v1, GenericRecord v2) throws Exception {
		// TODO Auto-generated method stub
		return v1;
	}
}
