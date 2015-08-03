import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class CSVProducer {
	private static Producer<String, String> producer;
	private final Properties properties = new Properties();

	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 9092;

	public CSVProducer(final String host, final int port) {
		properties.put("metadata.broker.list", host + ":" + port);
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("request.required.acks", "1");
		producer = new Producer<>(new ProducerConfig(properties));
	}

	public static void main(final String[] args) throws Exception {

		System.out.println("Usage: <csv file> <topic> <hostname> <port>");

		final String csvfile = args[0];
		final String topic = args[1];

		String host = null;
		if (args.length >= 3) {
			host = args[2];
		} else {
			host = DEFAULT_HOST;
		}

		int port;

		if (args.length >= 4) {
			port = Integer.valueOf(args[3]);
		} else {
			port = DEFAULT_PORT;
		}

		new CSVProducer(host, port);

		final File csv = new File(csvfile);
		final FileInputStream fis = new FileInputStream(csv);

		final BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		int count = 0;

		while ((line = br.readLine()) != null) {
			final KeyedMessage<String, String> data = new KeyedMessage<>(topic,
					line);
			producer.send(data);
			count++;
		}

		br.close();
		producer.close();

		System.out.println(count + " messages sent to topic: " + topic + " at "
				+ host + " : " + port);
	}
}
