package spark_aggregator.round;

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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import spark_aggregator.round.sink.CouchbaseSink;
import spark_aggregator.round.sink.Sink;
import spark_aggregator.round.sparkoperations.Filter;
import spark_aggregator.round.sparkoperations.RoundDeDuplicator;
import spark_aggregator.round.sparkoperations.RoundIdMapper;
import spark_aggregator.round.sparkoperations.ValueWriter;
//TO DO
//look at all intervals
//handle Offsets out of range with no configured reset policy for partitions
//use connection pool
//check for //HACKs
//
public class AggregatorInit {
	static Properties taskConfig;
	static Map<String, Object> kafkaParams;
	static SparkConf sparkConf;
	static JavaStreamingContext roundSsc;
	static Map<TopicPartition, Long> fromOffsets;
	static Sink sink;
	static String LEAGUETOPIC;
	static String ROUNDTOPIC;

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
		//kafkaParams.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		
		kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);
		kafkaParams.put("key.deserializer", KafkaAvroDeserializer.class);
	}
	void getAndUpdateOffsets(){
		fromOffsets = sink.getAndUpdateOffsets();
	}
	void setupSsc(){
		sparkConf = new SparkConf().setAppName("spark-round-aggregation")
				.registerKryoClasses(new Class<?>[]{GenericData.class});
		setupSink();

		roundSsc = new JavaStreamingContext(sparkConf, 
	    		Durations.seconds(Long.parseLong(taskConfig.getProperty("spark.batch.interval"))));
		
		fromOffsets = new HashMap<>();
		getAndUpdateOffsets();
		Collection<String> topics = Arrays.asList(ROUNDTOPIC);
//      optimisation
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
		if (fromOffsets.size() < 1){
			kafkaParams.put("auto.offset.reset", "earliest");
			kafkaStream = KafkaUtils.createDirectStream(roundSsc, 
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<Long, GenericRecord>Subscribe(topics, kafkaParams));
		}
		else
			kafkaStream = KafkaUtils.createDirectStream(roundSsc, 
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<Long, GenericRecord>Subscribe(topics, kafkaParams, fromOffsets));		
		
		
		kafkaStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Long, GenericRecord>>>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			private void display_partition_stuff(OffsetRange[] offsetRanges){
				for(OffsetRange  ofs: offsetRanges){
					System.out.println(ofs.topicPartition() + "  " + ofs.fromOffset() + " " + ofs.untilOffset());
				}
			}

			@Override
			public void call(JavaRDD<ConsumerRecord<Long, GenericRecord>> rdd) throws Exception {
			    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			    //display_partition_stuff(offsetRanges);
			    rdd
			    .filter(new Filter(Long.parseLong((String)taskConfig.get("game.id"))))
			    .mapToPair(new RoundIdMapper())
			    .reduceByKey(new RoundDeDuplicator())																														
				.foreachPartition(new ValueWriter(sink, offsetRanges));
			    
			    //((CanCommitOffsets) kafkaStream.inputDStream()).commitAsync(offsetRanges);	
			    //sink.upsert(offsetRanges);	
			}
			
		});
	}
	void setupSink(){
		sink = new CouchbaseSink(taskConfig);
	}
	public static void main(String[] args) throws InterruptedException{
		if (args.length != 1) {
			System.out.println("Please provide command line arguments: Path to configuration");
			System.exit(-1);
		}
		AggregatorInit init = new AggregatorInit();
		
		String configPath = args[0];
		init.buildTaskConfig(configPath);
		ROUNDTOPIC = taskConfig.getProperty("round.topic");
		LEAGUETOPIC = taskConfig.getProperty("league.topic");
		init.buildKafkaParams();
		
		init.setupSsc();
		
		roundSsc.start();
		roundSsc.awaitTermination();
	}		
}
