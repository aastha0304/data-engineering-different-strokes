package spark_aggregator.league.utils;

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import scala.Tuple2;
public class SchemaHandler implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Broadcast<Tuple2<String, String>> instance	;
	private SchemaHandler() {
	}
	public static Broadcast<Tuple2<String, String>> getInstance(JavaSparkContext jsc, String url) {
		if (instance == null) {
		      synchronized (ProductClusterMapping.class) {
		    	  if (instance == null) {
		    		  Schema valueSchema = fixSchema(Constants.VALUESCHEMAPREFIX, url);
		    		  Schema keySchema = fixSchema(Constants.KEYSCHEMAPREFIX, url);
		    		  instance = jsc.broadcast(new Tuple2<>(keySchema.toString(), valueSchema.toString()));
		    	  }
		      }
		}
		return instance;
	}
	private static Schema fixSchema(String suffix, String schemaUrl) {
		String subject = new StringBuffer().append(Constants.LEAGUETOPIC).append(suffix).toString();
		Integer identityMapCapacity;
		io.confluent.kafka.schemaregistry.client.rest.entities.Schema response;
		identityMapCapacity = 100;
		try {
			final RestService restService = new RestService(schemaUrl);
			CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(
					restService, identityMapCapacity);
			response = restService.getLatestVersion(subject);
			return cachedSchemaRegistryClient.getBySubjectAndID(subject,
					response.getId());
		} catch (IOException | RestClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}
}
