package spark_aggregator.league.sparkoperations;

import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;
import static org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import spark_aggregator.league.utils.Constants;

public class LeagueUserIndexer implements PairFunction<ConsumerRecord<Long,GenericRecord>, 
Tuple2<Long,Long>, GenericRecord>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HashMap<Long, Long> p2c;
	private String schemaString;
	private Schema valueSchema;

	private void fixSchema() {
		Schema.Parser parser = new Schema.Parser();
		this.valueSchema = parser.parse(this.schemaString);
	}
	public LeagueUserIndexer(HashMap<Long, Long> p2c, Tuple2<String,String> schema){
		this.p2c = p2c;
		this.schemaString = schema._2;
	}
	private byte[] recordToByteArray(GenericRecord record) throws IOException {
	    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
	      Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
	      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
	      writer.write(record, encoder);
	      byte[] byteArray = out.toByteArray();
	      return byteArray;
	    }
	  }
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Tuple2<Tuple2<Long, Long>, GenericRecord> call(ConsumerRecord<Long, GenericRecord> arg0) throws Exception {
		// TODO Auto-generated method stub
		fixSchema();
		GenericRecord record = arg0.value();
		if (!record.getSchema().equals(valueSchema)) {
			if (checkReaderWriterCompatibility(valueSchema, record.getSchema()).getType() != COMPATIBLE) {
		      return null;
		    }

		    try {
				BinaryDecoder decoder = new DecoderFactory().binaryDecoder(recordToByteArray(record), null);
				DatumReader<GenericRecord> reader = new GenericDatumReader<>(record.getSchema(), valueSchema);
				record = reader.read(null, decoder);
		    } catch (IOException e) {
		      throw new IOException(
		          String.format("Cannot convert avro record to new schema. Origianl schema = %s, new schema = %s",
		              record.getSchema(), valueSchema),
		          e);
		    }
		}
		//Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(valueSchema);

		//GenericRecord record = recordInjection.invert(out.toByteArray()).get();
		record.put(Constants.CLUSTERID_KEY, p2c.get(record.get(Constants.PRODUCTID_KEY)));
		//System.out.println(record.toString());
		return new Tuple2(new Tuple2(record.get(Constants.LEAGUEID_KEY),record.get(Constants.USERID_KEY)), record);
	}
}
