package ConfluentProd.adhocProd;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class NewEventsSchema {
	private String topic;
	private String regUrl;
	//private Schema keySchema;
	private Schema valueSchema;

	// private Properties taskConfig;
	NewEventsSchema(String topic, String regUrl) {
		this.topic = topic;
		this.regUrl = regUrl;

	}

	public void buildSchema() {
		this.valueSchema = fixSchema(topic, "-value");
		//this.keySchema = fixSchema(topic, "-key");
	}

	private Schema fixSchema(String topic, String suffix) {
		String subject = new StringBuffer().append(topic).append(suffix).toString();
		Integer identityMapCapacity;
		CachedSchemaRegistryClient cachedSchemaRegistryClient;
		io.confluent.kafka.schemaregistry.client.rest.entities.Schema response;
		identityMapCapacity = 100;
		try {
			final RestService restService = new RestService(this.regUrl);
			cachedSchemaRegistryClient = new CachedSchemaRegistryClient(
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

	public Schema getValueSchema() {
		return this.valueSchema;
	}

//	public Schema getKeySchema() {
//		return this.keySchema;
//	}

	public Type getValueSchemaType() {
		return this.valueSchema.getType();
	}

//	public Type getKeySchemaType() {
//		return this.keySchema.getType();
//	}
}
