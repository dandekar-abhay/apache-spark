package my.sample.java.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkWC {

	SparkSession sparkSession;

	public static void main(String[] args) {
		// Get an instance of spark-conf, required to build the spark session
		SparkConf conf = new SparkConf();
		
		// Set the master to local[2], you can set it to local
		// Note : For setting master, you can also use SparkSession.builder().master()
		// I preferred using config, which provides us more flexibility
		conf.setMaster("local[2]");
		// Create the session using the config above
		
		// getOrCreate : Gets an existing SparkSession or, if there is no existing one,
		// creates a new one based on the options set in this builder.
		SparkSession session = SparkSession.builder().getOrCreate();
		System.out.println("Session created");
		// Read the input file, and create a RDD from it
		JavaRDD<Row> lines = session.read().text("/tmp/README.md").javaRDD();

		// Split the lines into words
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.getString(0).split(" ")).iterator());

		// Print lines
		System.out.println(lines.count());
		// Print words
		System.out.println(words.count());
	}
}
