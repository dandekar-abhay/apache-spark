package apache.spark.poc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import apache.spark.poc.config.Configuration;

import java.util.Arrays;

public class SparkStructuredStreamingKafka {

	{
		Logger logger = Logger.getRootLogger();
		logger.setLevel(Level.DEBUG);
	}
	
	public static void main(String[] args) throws StreamingQueryException {
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("JavaStructuredKafkaWordCount")
				  .master("local[4]")
				  // .master("spark://fusionops-1:7077")				  
				  .config("spark.executor.memory", "2g")
//				  .config("spark.eventLog.enabled", "false")
				  .getOrCreate();
		
		
		System.out.println("Spark session created successfully");
		
//		StructType schemaType = new StructType()
//				.add("event_id", "long")
//				.add("sender_id", "long");
		
	    // Create DataSet representing the stream of input lines from kafka
	    Dataset<String> lines = spark
	      .readStream()
//	      .schema(schemaType)
	      .format("kafka")
	      .option("kafka.bootstrap.servers", Configuration.KAFKA_BROKER)
	      .option("subscribe", Configuration.KAFKA_TOPIC)
	      .load()
	      .selectExpr("CAST(value AS STRING)")
	      .as(Encoders.STRING());
		
	    System.out.println("Spark stream read");
	    
	    // Generate running word count
	    Dataset<Row> wordCounts = lines.flatMap(
	        (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(":")).iterator(),
	        Encoders.STRING()).groupBy("value").count();
		
	    System.out.println("Spark : running wc");
	    
	    // Start running the query that prints the running counts to the console
	    StreamingQuery query = wordCounts.writeStream()
	      .outputMode("complete")
	      .format("console")
	      .start();
	    
	    System.out.println("Spark writing to console :" + query.status());
	    
	    query.awaitTermination();
		
	}

}
