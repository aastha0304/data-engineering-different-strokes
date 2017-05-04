package spark_aggregator.league;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
import scala.Tuple2;
import spark_aggregator.league.sink.CouchbaseSink;
import spark_aggregator.league.sink.Sink;
import spark_aggregator.league.sparkoperations.LeagueUserIndexer;
import spark_aggregator.league.sparkoperations.PaidUserFilter;
import spark_aggregator.league.sparkoperations.RoundClusterIndexer;
import spark_aggregator.league.sparkoperations.RoundClusterReducer;
import spark_aggregator.league.sparkoperations.RoundIndexer;
import spark_aggregator.league.sparkoperations.RoundReducer;
import spark_aggregator.league.sparkoperations.ValueWriter;
import spark_aggregator.league.utils.FeeSizeRanges;
import spark_aggregator.league.utils.ProductClusterMapping;
//TO DO
//look at all intervals
//handle Offsets out of range with no configured reset policy for partitions
//use connection pool
//check for //HACKs

public class AggregatorInit {
	static Properties taskConfig;
	static Map<String, Object> kafkaParams;
	static SparkConf sparkConf;
	static JavaStreamingContext leagueSsc;
	static Map<TopicPartition, Long> fromOffsets;
	static Sink sink;
	
	static String LEAGUETOPIC;

	void buildTaskConfig(String fileName) {
		taskConfig = new Properties();
		try {
			InputStream in = new FileInputStream(fileName);
			taskConfig.load(in);
		} catch (IOException e) {
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
		kafkaParams.put("auto.offset.rest", "latest");
		//kafkaParams.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		
		kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);
		kafkaParams.put("key.deserializer", KafkaAvroDeserializer.class);
	}
	void getAndUpdateOffsets(){
		fromOffsets = sink.getAndUpdateOffsets();
	}
	void setupSsc(final String productClusterPath){
		sparkConf = new SparkConf().setAppName("spark-league-aggregation")
				.set("spark.executor.cores", "1")
				.registerKryoClasses(new Class<?>[]{GenericData.class});
		setupSink();

		leagueSsc = new JavaStreamingContext(sparkConf, 
	    		Durations.seconds(Long.parseLong(taskConfig.getProperty("spark.batch.interval"))));
		
		fromOffsets = new HashMap<>();
		getAndUpdateOffsets();
				
		Collection<String> topics = Arrays.asList(LEAGUETOPIC);
//      uncomment if parallelization needed
//		int numStreams = Runtime.getRuntime().availableProcessors()-1;
//		List<JavaDStream<ConsumerRecord<Long, GenericRecord>>> kafkaStreams = new ArrayList<>(numStreams);
//		for (int i = 0; i < numStreams; i++) {
//			fromOffsets.put(new TopicPartition(Constants.LEAGUETOPIC, i), (long)i);
//			kafkaStreams.add(KafkaUtils.createDirectStream(leagueSsc, 
//					LocationStrategies.PreferConsistent(),
//					ConsumerStrategies.<Long, GenericRecord>Subscribe(topics, kafkaParams, fromOffsets)));
//			fromOffsets.clear();
//		}
//		
//		JavaDStream<ConsumerRecord<Long, GenericRecord>> initialStream = leagueSsc.union(kafkaStreams.get(0), 
//			kafkaStreams.subList(1, kafkaStreams.size()));		
		final JavaInputDStream<ConsumerRecord<Long, GenericRecord>> kafkaStream;

		if (fromOffsets.size() < 1){ 
			kafkaParams.put("auto.offset.reset", "earliest");
			kafkaStream = KafkaUtils.createDirectStream(leagueSsc, 
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<Long, GenericRecord>Subscribe(topics, kafkaParams));
        }
		else{
			kafkaStream = KafkaUtils.createDirectStream(leagueSsc, 
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<Long, GenericRecord>Subscribe(topics, kafkaParams, fromOffsets));
		}
		final Broadcast<List<FeeSizeRanges>> p2cMappings = ProductClusterMapping.getInstance(leagueSsc.sparkContext(), 
				productClusterPath);
		kafkaStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Long, GenericRecord>>>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			
			@Override
			public void call(JavaRDD<ConsumerRecord<Long, GenericRecord>> rdd) throws Exception {
			    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

			    JavaPairRDD<Tuple2<Long, Long>, GenericRecord> mainRDD = rdd		    
			    .filter(new PaidUserFilter())
			    .mapToPair(new LeagueUserIndexer(p2cMappings.value()));
			    //not needed
			    //.reduceByKey(new LeagueUserDeduplicator())
				
			    JavaPairRDD<String, UsableColumns> roundRDD = mainRDD
			    		.mapToPair(new RoundIndexer())
			    		.reduceByKey(new RoundReducer());
			    
			    JavaPairRDD<String, UsableColumns> roundClusterRDD = mainRDD
			    		.mapToPair(new RoundClusterIndexer())
			    		.reduceByKey(new RoundClusterReducer());
			    
			    roundClusterRDD
			    		.union(roundRDD)
			    		.foreachPartition(new ValueWriter(sink, offsetRanges));
			    //in case there are issues with offsets committing to sink 
			    //((CanCommitOffsets) kafkaStream.inputDStream()).commitAsync(offsetRanges);	
			    
			    //don't do this, rdd line may fail but this execution will happen anyways
			    //sink.upsert(offsetRanges);	
			}
			
		});
	}
	void setupSink(){
		sink = new CouchbaseSink(taskConfig);
	}
	public static void main(String[] args) throws InterruptedException{
		if (args.length != 2) {
			System.out.println("Please provide command line arguments: Path to configuration, path to bucketInfo");
			System.exit(-1);
		}
		AggregatorInit init = new AggregatorInit();
		
		String configPath = args[0];
		init.buildTaskConfig(configPath);
		LEAGUETOPIC = taskConfig.getProperty("topic");
		
		init.buildKafkaParams();
		
		init.setupSsc(args[1]);
		
		leagueSsc.start();
		leagueSsc.awaitTermination();
	}		
}
