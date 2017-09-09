package apache.spark.poc;

import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
// import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import apache.spark.poc.config.Configuration;

public class SparkConsumer {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {

		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

		Map<String, Integer> topicThreadsMap = new HashMap<>();
		topicThreadsMap.put(Configuration.KAFKA_TOPIC, 2);

		JavaPairDStream<String, String> messages = KafkaUtils.createStream(jssc, Configuration.KAFKA_ZK_QUORUM,
				Configuration.KAFKA_GROUP_ID, topicThreadsMap);

		JavaDStream<String> lines = messages.map(Tuple2::_2);
		
		lines.print();
		
		System.out.println(lines.count());
		

		// JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

		// JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.

//		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
//				.reduceByKey((prevCount, nextCount) -> prevCount + nextCount);
//
//		wordCounts.print();
		
		jssc.start();
		jssc.awaitTermination();

	}

}
