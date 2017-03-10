package spark_aggregator.league.sink;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;
import spark_aggregator.league.UsableColumns;


public interface Sink {
	Map<TopicPartition, Long> getAndUpdateOffsets();
	void upsert(Tuple2<Tuple2<Long, Long>, UsableColumns> row);
	void upsert(OffsetRange[] offsetRanges);
}
