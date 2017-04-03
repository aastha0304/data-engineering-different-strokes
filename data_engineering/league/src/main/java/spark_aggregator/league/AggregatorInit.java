package spark_aggregator.league;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import spark_aggregator.league.sink.CouchbaseSink;
import spark_aggregator.league.sink.Sink;
import spark_aggregator.league.sparkoperations.LeagueUserDeduplicator;
import spark_aggregator.league.sparkoperations.LeagueUserIndexer;
import spark_aggregator.league.sparkoperations.PaidUserFilter;
import spark_aggregator.league.sparkoperations.RoundClusterIndexer;
import spark_aggregator.league.sparkoperations.RoundClusterReducer;
import spark_aggregator.league.sparkoperations.ValueWriter;
import spark_aggregator.league.utils.Constants;
import spark_aggregator.league.utils.ProductClusterMapping;
//TO DO
//look at all intervals
//increase write intervals
//look at offset
public class AggregatorInit {
	Properties taskConfig;
	static Map<String, Object> kafkaParams;
	static SparkConf sparkConf;
	static JavaStreamingContext leagueSsc;
	static Map<TopicPartition, Long> fromOffsets;
	static Sink sink;

	void buildTaskConfig(String fileName) {
		taskConfig = new Properties();
		try {
			InputStream in = new FileInputStream(fileName);
			taskConfig.load(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}
	void buildKafkaParams(){	
		Random random = new Random();
		kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", taskConfig.getProperty("bootstrap.servers"));
		kafkaParams.put("schema.registry.url", taskConfig.getProperty("schema.registry.url"));

		kafkaParams.put("group.id", taskConfig.getProperty("group.id") 
				+ random.nextInt() 
				+ "-" + System.currentTimeMillis());
		kafkaParams.put("enable.auto.commit", false);
		//kafkaParams.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		
		kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);
		kafkaParams.put("key.deserializer", KafkaAvroDeserializer.class);
	}
	void getAndUpdateOffsets(){
		fromOffsets = sink.getAndUpdateOffsets();
		if (fromOffsets.size() < 1){ 
            /*
             * TO DO
         It would be better to ask Kafka for the number of partitions so we can still
         build this Map dynamically when there isn't any data in the database yet.
          */
            fromOffsets.put(new TopicPartition(Constants.LEAGUETOPIC, 0), 0L);
        }
	}
	void setupSsc(final String productClusterPath){
		sparkConf = new SparkConf().setAppName("spark-league-aggregation")
// 		uncomment for tweaking configs				
//				.set("spark.streaming.kafka.consumer.poll.ms", "70000")
//				.set("spark.streaming.kafka.consumer.cache.initialCapacity", "1")
//				.set("spark.streaming.kafka.consumer.cache.maxCapacity","1")
				.registerKryoClasses(new Class<?>[]{GenericData.class});
		setupSink();

		leagueSsc = new JavaStreamingContext(sparkConf, 
	    		Durations.seconds(Long.parseLong(taskConfig.getProperty("spark.batch.interval"))));
		
		fromOffsets = new HashMap<>();
		getAndUpdateOffsets();
				
		Collection<String> topics = Arrays.asList(Constants.LEAGUETOPIC);
//      uncomment if parallelization needed
//		int numStreams = Runtime.getRuntime().availableProcessors()-1;
//		List<JavaDStream<ConsumerRecord<Long, GenericRecord>>> kafkaStreams = new ArrayList<>(numStreams);
//		for (int i = 0; i < numStreams; i++) {
//			kafkaStreams.add(KafkaUtils.createDirectStream(leagueSsc, 
//					LocationStrategies.PreferConsistent(),
//					ConsumerStrategies.<Long, GenericRecord>Subscribe(topics, kafkaParams, fromOffsets)));
//		}
//		
//		JavaDStream<ConsumerRecord<Long, GenericRecord>> initialStream = leagueSsc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));		
		final JavaInputDStream<ConsumerRecord<Long, GenericRecord>> kafkaStream;
		
		kafkaParams.put("auto.offset.reset", "latest");
		final Broadcast<HashMap<Long, Long>> p2cMappings = ProductClusterMapping.getInstance(leagueSsc.sparkContext(), 
				productClusterPath);

		kafkaStream = KafkaUtils.createDirectStream(leagueSsc, 
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<Long, GenericRecord>Subscribe(topics, kafkaParams, fromOffsets));
				
		kafkaStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Long, GenericRecord>>>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<ConsumerRecord<Long, GenericRecord>> rdd) throws Exception {
				// TODO Auto-generated method stub
			    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			    rdd
			    .filter(new PaidUserFilter())
				.mapToPair(new LeagueUserIndexer(p2cMappings.value()))
			    .reduceByKey(new LeagueUserDeduplicator())
				.mapToPair(new RoundClusterIndexer())
				.reduceByKey(new RoundClusterReducer())
				.foreachPartition(new ValueWriter(sink, offsetRanges));
			    //in case there are issues with offsets committing to sink 
			    //((CanCommitOffsets) kafkaStream.inputDStream()).commitAsync(offsetRanges);	
			    
			    //don't do this, rdd line may fail but this execution will happen anyways
			    //sink.upsert(offsetRanges);	
			}
			
		});
	}
	void setupSink(){
		//TO DO
		sink = new CouchbaseSink(taskConfig);
	}
	public static void main(String[] args) throws InterruptedException{
		if (args.length != 2) {
			System.out.println("Please provide command line arguments: Path to configuration, path to product cluster mapping");
			System.exit(-1);
		}
		AggregatorInit init = new AggregatorInit();
		
		String configPath = args[0];
		init.buildTaskConfig(configPath);
		
		init.buildKafkaParams();
		
		init.setupSsc(args[1]);
		
		leagueSsc.start();
		leagueSsc.awaitTermination();
	}		
}
