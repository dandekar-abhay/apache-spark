package apache.spark.poc.kafka.producer;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import apache.spark.poc.config.Configuration;

public class NotificationProducer {
	
	private static final boolean debug = true; 

	public static void main(String[] argv) throws Exception {

		final String topicName = Configuration.KAFKA_TOPIC;

		// Configure the Producer
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_BROKER);
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put("request.required.acks","1");

		configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, IntegerPartitioner.class.getCanonicalName());
		configProperties.put("partitions.0", "0");
		configProperties.put("partitions.1", "1");
		configProperties.put("partitions.2", "2");
		configProperties.put("partitions.3", "3");

		final Producer<String, String> producer = new KafkaProducer<String, String>(configProperties);
		System.out.println("Sending messages to topic: " + topicName);

		Timer timer = new Timer();
		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				try {
					// int randomNum = ThreadLocalRandom.current().nextInt(0, 5);
					int randomNum = 100;
					final String msg = randomNum + ":" + 1;
					producer.send(new ProducerRecord<String, String>(topicName, msg));
					if(debug) {
						System.out.println("Message inserted : " + msg);
					}
				} catch (Exception e) {
					System.err.println("Exception while calling the timer");
					e.printStackTrace(System.err);
				}
			}
		};
		
		timer.schedule(task, 1000, Configuration.KAFKA_PRODUCER_FREQ_SECS * 1000);		
	}
}