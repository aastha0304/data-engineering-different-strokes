package ConfluentProd.adhocProd;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.Long;
public class ConsumerRunner implements Runnable {
	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;
	SingleProducer prodObj;
	private List<String> toParse;
	static JSONParser parser = new JSONParser();

	public ConsumerRunner(int id, String groupId, List<String> topics,
			SingleProducer prodObj, String bootstrapServers, String[] strings, String regUrl) {
		this.id = id;
		this.topics = topics;
		this.prodObj = prodObj;
		this.toParse = Arrays.asList(strings);
		Properties consumerProps = new Properties();
		consumerProps.put("bootstrap.servers", bootstrapServers);
		consumerProps.put("group.id", groupId);
		consumerProps.put("auto.offset.reset", "earliest");
		consumerProps.put("enable.auto.commit", "false");
		consumerProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put("schema.registry.url", regUrl);
		consumer = new KafkaConsumer<>(consumerProps);
	}
	private static boolean stringContainsItemFromList(String inputStr, List<String> toParse)
	{
	    for(int i =0; i < toParse.size(); i++)
	    {
	        if(inputStr.contains(toParse.get(i)))
	        {
	            return true;
	        }
	    }
	    return false;
	}
	private JSONArray parseOldRecord(ConsumerRecord<String, String> record) {
		Object maps = new JSONObject();
		JSONArray mapsArray = new JSONArray();
		if( stringContainsItemFromList(record.value(), this.toParse) ) {
			try {
				maps = parser.parse(record.value());
				if( maps instanceof JSONArray ){
					mapsArray = (JSONArray) maps;
					int len = mapsArray.size();
					if(len>0){
						return mapsArray;
					}
				}else if (maps instanceof JSONObject){
					mapsArray.add(maps);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return mapsArray;
	}
	@Override
	public void run() {
		ConsumerRecords<String, String> records = new ConsumerRecords<String, String>(null);
		try {
			consumer.subscribe(this.topics);
			while (true) {
				records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					JSONArray oldRecord = parseOldRecord(record);
					if( oldRecord.size()>0 ){
						for(int i=0; i<oldRecord.size(); i++){
							JSONObject singleRecord = (JSONObject) oldRecord.get(i);
							if( !singleRecord.isEmpty() ){
								//JSONObject single = (JSONObject) parser.parse(singleRecord);
								prodObj.produceNewEvents(singleRecord);
							}
						}	
					}
				}
				consumer.commitAsync();
			}
		}catch(ClassCastException e){
		    e.printStackTrace();
		}catch (Exception e) {
		    e.printStackTrace();
		}finally {    
			consumer.close();
			this.prodObj.shutdown();
			System.exit(1);
		}
	}
	public void shutdown() {
		consumer.wakeup();
	}
}
