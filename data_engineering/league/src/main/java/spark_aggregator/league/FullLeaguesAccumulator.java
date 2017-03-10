package spark_aggregator.league;
import java.util.HashSet;

import org.apache.spark.api.java.JavaSparkContext;
import  org.apache.spark.util.AccumulatorV2;
public class FullLeaguesAccumulator extends AccumulatorV2<Long, HashSet<Long>>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	HashSet<Long> fullLeagues;
	
	private static volatile FullLeaguesAccumulator instance = null;

	  public static FullLeaguesAccumulator getInstance(JavaSparkContext jsc) {
	    if (instance == null) {
	      synchronized (FullLeaguesAccumulator.class) {
	        if (instance == null) {
	          instance = new FullLeaguesAccumulator()     ;
	          jsc.sc().register(instance, "fullLeaguesAccumulator");
	        }
	      }
	    }
	    return instance;
	  }
	  
	FullLeaguesAccumulator(){
		fullLeagues = new HashSet<>();
	}
	
	FullLeaguesAccumulator(Long initial){
		this();
		fullLeagues.add(initial);
	}
	
	@Override
	public void add(Long arg0) {
		// TODO Auto-generated method stub
		fullLeagues.add(arg0);
	}

	@Override
	public AccumulatorV2<Long, HashSet<Long>> copy() {
		// TODO Auto-generated method stub
		FullLeaguesAccumulator copy = new FullLeaguesAccumulator();
		copy.fullLeagues.addAll(this.fullLeagues);
		return copy;
	}

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
		return fullLeagues.size() == 0;
	}

	@Override
	public void merge(AccumulatorV2<Long, HashSet<Long>> arg0) {
		// TODO Auto-generated method stub
		fullLeagues.addAll(arg0.value());
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		fullLeagues.clear();
	}

	@Override
	public HashSet<Long> value() {
		// TODO Auto-generated method stub
		return fullLeagues;
	}

}
