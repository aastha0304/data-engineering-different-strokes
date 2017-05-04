package spark_aggregator.league.utils;

import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;
import static org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
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
	private static String topic;
	private SchemaHandler() {
	}
	public static Broadcast<Tuple2<String, String>> getInstance(JavaSparkContext jsc, String url, String topic) {
		if (instance == null) {
		      synchronized (SchemaHandler.class) {
		    	  if (instance == null) {
		    		  Schema valueSchema = fixSchema(Constants.VALUESCHEMAPREFIX, url);
		    		  Schema keySchema = fixSchema(Constants.KEYSCHEMAPREFIX, url);
		    		  instance = jsc.broadcast(new Tuple2<>(keySchema.toString(), valueSchema.toString()));
		    	  }
		      }
		}
		SchemaHandler.topic = topic;
		return instance;
	}
	private static Schema fixSchema(String suffix, String schemaUrl) {
		String subject = new StringBuffer().append(topic).append(suffix).toString();
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
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}
	/**
	   * Convert a GenericRecord to a byte array.
	   */
	private static byte[] recordToByteArray(GenericRecord record) throws IOException {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
			writer.write(record, encoder);
			byte[] byteArray = out.toByteArray();
			return byteArray;
		}
	}
	private static Schema parseSchema(String schemaStr) {
		Schema.Parser parser = new Schema.Parser();
		return parser.parse(schemaStr);
	}
	public static GenericRecord convertRecordSchema(GenericRecord record, String newSchemaStr) throws IOException {
		Schema newSchema = parseSchema(newSchemaStr);
		
	    if (record.getSchema().equals(newSchema)) {
	      return record;
	    }

	    if (checkReaderWriterCompatibility(newSchema, record.getSchema()).getType() != COMPATIBLE) {
	      return null;
	    }

	    try {
	      BinaryDecoder decoder = new DecoderFactory().binaryDecoder(recordToByteArray(record), null);
	      DatumReader<GenericRecord> reader = new GenericDatumReader<>(record.getSchema(), newSchema);
	      return reader.read(null, decoder);
	    } catch (IOException e) {
	      throw new IOException(
	          String.format("Cannot convert avro record to new schema. Origianl schema = %s, new schema = %s",
	              record.getSchema(), newSchema),
	          e);
	    }
	  }
}	
