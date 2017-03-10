package spark_aggregator.league.sparkoperations;

import java.io.Serializable;

import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import spark_aggregator.league.UsableColumns;
import spark_aggregator.league.sink.Sink;

public class ValueWriter implements VoidFunction<Tuple2<Tuple2<Long, Long>, UsableColumns>>, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Sink sink;
	
	public ValueWriter(Sink sink){
		this.sink = sink;
	}
	
	@Override
	public void call(Tuple2<Tuple2<Long, Long>, UsableColumns> row) throws Exception {
		//System.out.println(row._1._1);
		sink.upsert(row);
	}
}
