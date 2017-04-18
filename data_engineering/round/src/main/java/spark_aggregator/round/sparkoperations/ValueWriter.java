package spark_aggregator.round.sparkoperations;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;
import spark_aggregator.round.RoundMeta;
import spark_aggregator.round.sink.Sink;

public class ValueWriter implements VoidFunction<Iterator<Tuple2<Long,RoundMeta>>>, Serializable{
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
	public void call(Iterator<Tuple2<Long, RoundMeta>> t) throws Exception {
		// TODO Auto-generated method stub
		this.sink.upsert(t, null);
	}
}
