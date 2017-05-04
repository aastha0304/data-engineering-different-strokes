package spark_aggregator.league.sparkoperations;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;
import spark_aggregator.league.UsableColumns;
import spark_aggregator.league.sink.Sink;

public class ValueWriter implements VoidFunction<Iterator<Tuple2<String, UsableColumns>>>, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Sink sink;
	OffsetRange[] offsetRanges;
	public ValueWriter(Sink sink, OffsetRange[] offsetRanges){
		this.sink = sink;
		this.offsetRanges = offsetRanges;
	}
	
	@Override
	public void call(Iterator<Tuple2<String, UsableColumns>> t) throws Exception {
		if(offsetRanges!=null)
			this.sink.upsert(t, this.offsetRanges[TaskContext.get().partitionId()]);
		else
			this.sink.upsert(t, null);
	}
}
