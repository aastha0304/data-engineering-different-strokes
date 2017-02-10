package aggregationEngine.sparkAggregator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
public class AggregatorInit {
	static Properties taskConfig;
	static SparkConf scfg;
	static JavaStreamingContext ssc;
	static Map<String, Object> kafkaParams;

	private static NewEventsSchema schemaObj;
	private static Schema valueSchema;
	//private static Schema keySchema;
	static LongDeserializer keyDecoder;
	static KafkaAvroDeserializer valueDecoder;
	static Injection<GenericRecord, byte[]> valueInjection;
	static Injection<Long, byte[]> keyInjection;
	
	static void buildTaskConfig(String fileName) {
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
	static void buildStreamContext() throws ClassNotFoundException{
		scfg = new SparkConf().setAppName("sparkAggregator")
				.registerKryoClasses(
	                    new Class<?>[]{GenericData.class});
	                        
		long batchInterval = Long.parseLong( taskConfig.getProperty("spark.batch.interval") );
		ssc = new JavaStreamingContext(scfg, Durations.seconds(batchInterval));
	}
	static void buildKafkaParams(){
		Random random = new Random();
	
		kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", taskConfig.getProperty("bootstrap.servers"));
		kafkaParams.put("schema.registry.url", taskConfig.getProperty("schema.registry.url"));

		kafkaParams.put("group.id", taskConfig.getProperty("kafka.group.id") 
				+ random.nextInt() 
				+ "-" + System.currentTimeMillis());
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);
		//kafkaParams.put("specific.avro.reader", "true");
		
		Properties props = new Properties();
		props.putAll(kafkaParams);
		props.put("zookeeper.connect", taskConfig.getProperty("zookeeper.connect"));
		
		kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);
		kafkaParams.put("key.deserializer", LongDeserializer.class);
	}
	private static void buildSchema(){
		schemaObj = new NewEventsSchema(taskConfig.getProperty("topic"),
					taskConfig.getProperty("schema.registry.url"));
		schemaObj.buildSchema();
		valueSchema = schemaObj.getValueSchema();
		//keySchema = schemaObj.getKeySchema();
		valueInjection = GenericAvroCodecs.toBinary(valueSchema);
	}
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
		if (args.length != 1) {
			System.out.println("Please provide command line arguments: Path to configuration");
			System.exit(-1);
		}

		String fileName = args[0];
		buildTaskConfig(fileName);
		buildSchema();
		buildStreamContext();
		buildKafkaParams();
		
		String topic = (String) taskConfig.getProperty("topic");
		JavaInputDStream<ConsumerRecord<Long, GenericRecord>> stream = KafkaUtils.createDirectStream(ssc, 
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<Long, GenericRecord>Subscribe(topicToSet(topic), kafkaParams));
		stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Long, GenericRecord>>>(){
			@Override
			public void call(JavaRDD<ConsumerRecord<Long, GenericRecord>> arg0) throws Exception {
				// TODO Auto-generated method stub
				arg0.foreach(new VoidFunction<ConsumerRecord<Long, GenericRecord>>(){
					@Override
					public void call(ConsumerRecord<Long, GenericRecord> arg) throws Exception {
						// TODO Auto-generated method stub
						System.out.println(arg.value());
					}
				});
			}
		});
		ssc.start();
	    long startTime = System.currentTimeMillis();
	    ssc.awaitTermination();
	    System.out.println(System.currentTimeMillis() - startTime);
	}
	private static Collection<String> topicToSet(String topic) {
		return Arrays.asList(topic);
	}
}
