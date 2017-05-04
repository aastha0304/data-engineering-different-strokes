package spark_aggregator.league.sparkoperations;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark_aggregator.league.UsableColumns;
import spark_aggregator.league.utils.Constants;

public class RoundClusterIndexer implements 
	PairFunction<Tuple2<Tuple2<Long,Long>, GenericRecord>, String, UsableColumns>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	@Override
	public Tuple2<String, UsableColumns> call(Tuple2<Tuple2<Long, Long>, GenericRecord> arg0)
		throws Exception {
		UsableColumns usableColumns = new UsableColumns();
		usableColumns.setAmount((float) arg0._2.get(Constants.ENTRYFEE_KEY));
		usableColumns.setUsers(1);
		String key = new StringBuilder().append(String.valueOf(arg0._2.get(Constants.ROUNDID_KEY)))
				.append(':')
				.append(String.valueOf(arg0._2.get(Constants.PRODUCTID_KEY)))
				.toString();
		return new Tuple2<String, UsableColumns>(key, usableColumns);
	}
}
