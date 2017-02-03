package ConfluentProd.adhocProd;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

class SimpleProdCallback implements Callback {
	@Override
	public void onCompletion(RecordMetadata recordMetadata, Exception e) {
		if (e != null) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
