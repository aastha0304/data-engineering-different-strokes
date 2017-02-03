package ConfluentProd.adhocProd;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AdhocConProd {
	Properties taskConfig;
	List<ConsumerRunner> consumers;
	ExecutorService executor;

	public void buildConsumer(SingleProducer prodObj) {
		int numConsumers = 3;
		String groupId = taskConfig.getProperty("group.id");
		List<String> topics = Arrays.asList(taskConfig
				.getProperty("producingTopic"));
		executor = Executors.newFixedThreadPool(numConsumers);
		consumers = new ArrayList<>();
		for (int i = 0; i < numConsumers; i++) {
			ConsumerRunner consumer = new ConsumerRunner(i, groupId, topics,
					prodObj, this.taskConfig.getProperty("bootstrap.servers"),
					taskConfig.getProperty("toParse").split(","), taskConfig.getProperty("schema.registry.url"));
			consumers.add(consumer);
			executor.submit(consumer);
		}
	}

	void buildTaskConfig(String fileName) {
		this.taskConfig = new Properties();
		try {
			
			InputStream in = new FileInputStream(fileName);
			this.taskConfig.load(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}

	void runConProd() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerRunner consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out
					.println("Please provide command line arguments: Path to configuration");
			System.exit(-1);
		}
		AdhocConProd adhocConProd = new AdhocConProd();
		String fileName = args[0];
		adhocConProd.buildTaskConfig(fileName);

		NewEventsSchema schemaObj = new NewEventsSchema(
				adhocConProd.taskConfig.getProperty("topic"),
				adhocConProd.taskConfig.getProperty("schema.registry.url"));
		schemaObj.buildSchema();

		SingleProducer prodObj = SingleProducer.getInstance();
		prodObj.init(adhocConProd.taskConfig.getProperty("bootstrap.servers"),
				adhocConProd.taskConfig.getProperty("schema.registry.url"),
				schemaObj, adhocConProd.taskConfig.getProperty("topic"));

		adhocConProd.buildConsumer(prodObj);

		adhocConProd.runConProd();
	}
}

//mvn package
//mvn exec:java -Dexec.mainClass=POCConfluentProd.javaProd.AdhocConProd  -Dexec.args="/Users/aastha/codes/workspace/javaProd/src/main/resources/config.properties"