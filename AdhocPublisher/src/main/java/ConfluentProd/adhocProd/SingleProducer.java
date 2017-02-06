package ConfluentProd.adhocProd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

public class SingleProducer {
	private static SingleProducer instance = null;

	private KafkaProducer<Long, GenericRecord> producer;
	
	private NewEventsSchema schemaObj;
	List<Schema> schemas;
	Map<String, Integer> schemaMap;
	private String topic;
	
	private static final String COMPRESSION_TYPE_CONFIG = "snappy"; 
	private static final String ACKS_TYPE_CONFIG = "1"; 
	
	private SingleProducer() {
	      // Exists only to defeat instantiation.
	}
	public static SingleProducer getInstance() {
		if(instance == null) {
			instance = new SingleProducer();
		}
		return instance;
	}
	public void init(String bootstrapServers, String regUrl, NewEventsSchema schemaObj, String topic){
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
		        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
		        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		producerProps.put("schema.registry.url", regUrl);
		producerProps.put(ProducerConfig.ACKS_CONFIG, ACKS_TYPE_CONFIG);
		producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE_CONFIG); 
		
		this.producer = new KafkaProducer<>(producerProps);
		this.schemaObj = schemaObj;
		this.topic = topic;

		Schema innerSchema = this.schemaObj.getValueSchema().getField(this.topic).schema();
		if(innerSchema.getType()==Schema.Type.UNION){
			this.schemaMap = new HashMap<>();
			schemas = innerSchema.getTypes();
			int i = 0;
			for(Schema schema: schemas){
				this.schemaMap.put(schema.getName(), i++);
			}
		}
	}
	private String toCamelCase(String hyphened){
		String[] splits = hyphened.split("-");
		StringBuffer res = new StringBuffer().append(splits[0]);
		for( int i = 1; i < splits.length; i++){
			res.append(Character.toString(splits[i].charAt(0)).toUpperCase()).append(splits[i].substring(1));
		}
		return res.toString();
	}
	private double massageAmount(Object amount){
		double valueTwo=0;
		if(amount instanceof Long) 
			valueTwo = ((Long) amount).doubleValue();
		return valueTwo;
	}
	public void produceNewEvents(JSONObject oldRecord){
		if( oldRecord != null ){
			String eventName = toCamelCase( (String) oldRecord.get("eventName"));
			GenericRecord outer = new GenericData.Record(this.schemaObj.getValueSchema());
			GenericRecord actual = new GenericData.Record(this.schemas.get(schemaMap.get(eventName)));
			double amount = massageAmount(oldRecord.get("amount"));
			actual.put("userId", oldRecord.get("userId"));
			actual.put("amount", amount);
			actual.put("timestamp", oldRecord.get("timestamp"));
			outer.put(this.topic, actual);
			try{
				ProducerRecord<Long, GenericRecord> record = new ProducerRecord<>(this.topic, 
						(long) oldRecord.get("userId"), outer);
				producer.send(record, new SimpleProdCallback());
			}catch(Exception e){
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	public void shutdown(){
		this.producer.close();
	}
}
