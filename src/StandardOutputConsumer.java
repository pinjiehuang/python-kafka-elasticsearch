import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class StandardOutputConsumer {

	private static KafkaConsumer<String, String> consumer;
	private final Properties properties = new Properties();

	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 9092;

	public StandardOutputConsumer(final String host, final int port) {
		properties.put("bootstrap.servers", host + ":" + port);
		properties.put("group.id", "test");

		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		// properties.put("key.serializer",
		// "org.apache.kafka.common.serializers.StringSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serializers.StringSerializer");
		properties.put("partition.assignment.strategy", "range");
		properties.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(properties);
	}

	public static void main(final String[] args) throws InterruptedException {

		final String topic = args[0];

		String host = null;
		if (args.length >= 2) {
			host = args[1];
		} else {
			host = DEFAULT_HOST;
		}

		int port;
		if (args.length >= 3) {
			port = Integer.valueOf(args[2]);
		} else {
			port = DEFAULT_PORT;
		}

		new StandardOutputConsumer(host, port);
		consumer.subscribe(topic);
		while (true) {
			final Map<String, ConsumerRecords<String, String>> records = consumer
					.poll(1000);
			if (records != null && !records.isEmpty()) {
				System.out.println("Not empty");
			}
			// for (ConsumerRecord<String, String> record : records)
			// System.out.printf("offset = %d, key = %s, value = %s",
			// record.offset(), record.key(), record.value());
		}

	}
}
