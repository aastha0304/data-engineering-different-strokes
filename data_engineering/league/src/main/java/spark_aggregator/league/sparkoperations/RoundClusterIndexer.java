package spark_aggregator.league.sparkoperations;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark_aggregator.league.UsableColumns;
import spark_aggregator.league.utils.Constants;

//Tuple2<Tuple2<Long,Long>,GenericRecord>
public class RoundClusterIndexer implements 
	PairFunction<Tuple2<Tuple2<Long,Long>, GenericRecord>, Tuple2<Long,Long>, UsableColumns>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Tuple2<Tuple2<Long, Long>, UsableColumns> call(Tuple2<Tuple2<Long, Long>, GenericRecord> arg0)
		throws Exception {
		// TODO Auto-generated method stub
		UsableColumns usableColumns = new UsableColumns();
		usableColumns.setAmount((float) arg0._2.get(Constants.ENTRYFEE_KEY));
		Set<Long> fullLeagues = new HashSet<>();
		if( arg0._2.get(Constants.LEAGUESIZE_KEY) == arg0._2.get(Constants.CURRENTSIZE_KEY))
			fullLeagues.add((long) arg0._2.get(Constants.LEAGUEID_KEY));
		usableColumns.setFullLeagues(fullLeagues);
		usableColumns.setUsers(1);
		return new Tuple2(new Tuple2(arg0._2.get(Constants.ROUNDID_KEY), arg0._2.get(Constants.PRODUCTID_KEY)),
				usableColumns);
	}
}
