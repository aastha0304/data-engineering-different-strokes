package spark_aggregator.round.sink;

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;
import spark_aggregator.round.RoundMeta;


public interface Sink {
	Map<TopicPartition, Long> getAndUpdateOffsets();
	void upsert(Iterator<Tuple2<Long, RoundMeta>> t, OffsetRange o);
	void upsert(OffsetRange[] offsetRanges);
}
