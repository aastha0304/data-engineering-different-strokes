package spark_aggregator.league.sparkoperations;


import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;
import spark_aggregator.league.UsableColumns;

public class RoundClusterReducer implements Function2<UsableColumns, UsableColumns, UsableColumns>, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	@Override
	public UsableColumns call(UsableColumns arg0, UsableColumns arg1) throws Exception {
		// TODO Auto-generated method stub
		UsableColumns res = new UsableColumns();
		res.setUsers(arg0.getUsers()+arg1.getUsers());
		res.setAmount(arg0.getAmount()+arg1.getAmount());
		res.setFullLeagues(null);
		return res;
	}
}
