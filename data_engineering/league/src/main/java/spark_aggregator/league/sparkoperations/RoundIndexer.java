package spark_aggregator.league.sparkoperations;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark_aggregator.league.UsableColumns;
import spark_aggregator.league.utils.Constants;


public class RoundIndexer implements PairFunction<Tuple2<Tuple2<Long, Long>, GenericRecord>, String, UsableColumns>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, UsableColumns> call(Tuple2<Tuple2<Long, Long>, GenericRecord> arg0) throws Exception {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
				UsableColumns usableColumns = new UsableColumns();
				usableColumns.setAmount((float) arg0._2.get(Constants.ENTRYFEE_KEY));
				usableColumns.setUsers(1);

				Set<Long> fullLeagues = new HashSet<>();
				if( arg0._2.get(Constants.LEAGUESIZE_KEY) == arg0._2.get(Constants.CURRENTSIZE_KEY))
					fullLeagues.add((long) arg0._2.get(Constants.LEAGUEID_KEY));
				usableColumns.setFullLeagues(fullLeagues);
				
//				Map<Integer, UsableClusterColumns> clusters = new HashMap<>();
//				UsableClusterColumns cluster = new UsableClusterColumns();
//				cluster.setId((int)arg0._2.get(Constants.PRODUCTID_KEY));
//				cluster.setAmount((float) arg0._2.get(Constants.ENTRYFEE_KEY));
//				cluster.setUsers(1);
//				clusters.put(cluster.getId(), cluster);
//				usableColumns.setClusters(clusters);
				
//				return new Tuple2(new Tuple2(arg0._2.get(Constants.ROUNDID_KEY), arg0._2.get(Constants.PRODUCTID_KEY)),
//						usableColumns);
				return new Tuple2<String, UsableColumns>(String.valueOf(arg0._2.get(Constants.ROUNDID_KEY)), usableColumns);
	}

}
