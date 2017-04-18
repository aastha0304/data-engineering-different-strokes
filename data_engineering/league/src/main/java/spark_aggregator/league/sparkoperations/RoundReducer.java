package spark_aggregator.league.sparkoperations;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

import spark_aggregator.league.UsableColumns;

public class RoundReducer implements Function2<UsableColumns, UsableColumns, UsableColumns>, Serializable{

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
		arg0.getFullLeagues().addAll(arg1.getFullLeagues());
		res.setFullLeagues(arg0.getFullLeagues());
		
//		Map<Integer, UsableClusterColumns> a = arg0.getClusters();
//		Map<Integer, UsableClusterColumns> b = arg1.getClusters();
//		for(Entry<Integer, UsableClusterColumns> e:a.entrySet()){
//			int id = e.getKey();
//
//			if(b.containsKey(id)){
//				UsableClusterColumns cluster = a.get(id);
//				cluster.setAmount(cluster.getAmount()+b.get(id).getAmount());
//				cluster.setUsers(cluster.getUsers()+b.get(id).getUsers());
//				a.put(id, cluster);
//				b.remove(id);
//			}
//		}
//		for(Entry<Integer, UsableClusterColumns> e:b.entrySet()){
//				UsableClusterColumns cluster = b.get(e.getKey());
//				a.put(e.getKey(), cluster);
//		}
//		display(a);
//
//		res.setClusters(a);
		return res;
	}
	
}
