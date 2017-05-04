package spark_aggregator.league.sink;

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;
import spark_aggregator.league.UsableColumns;


public interface Sink {
	Map<TopicPartition, Long> getAndUpdateOffsets();
	void upsert(Iterator<Tuple2<String, UsableColumns>> row, OffsetRange o);
	void upsert(OffsetRange[] offsetRanges);
}
