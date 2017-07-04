package my.sample.java.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkWordCount implements Serializable {

	// WE NEED TO MAKE THIS SERIALIZABLE AS THIS CLASS WILL BE PASSED OVER 
	// THE WIRE
	private static final long serialVersionUID = 7040262147300391556L;

	public void process(JavaSparkContext jsc, String inputFilePath, String outputFilePath) {
		JavaRDD<String> text = jsc.textFile(inputFilePath);
		
		JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 2364418317972495932L;

			public Iterator<String> call(String oneLine) {
				return Arrays.asList(oneLine.split(" ")).iterator();
			}
		});

		long totalCount = words.count(); 
		System.out.println("Total words : " + totalCount);
		
		JavaPairRDD<String, Integer> counts = words.mapToPair(
					// This is the MAP FUNCTION
				new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = -7197270582238966886L;

					public Tuple2<String, Integer> call(String x){
						// SEND ALL THE STRINGS WITH 1 AS VALUE
						// TUPLE2 IS A SCALA CLASS, PROVIDED BY SPARK
						return new Tuple2<String, Integer>(x , 1);
					}
				}
				
				).reduceByKey(
						// This is the REDUCE FUNCTION
				new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 8330220763629802421L;

					public Integer call(Integer x, Integer y) throws Exception {
						return x + y;
					}
				});		

		// SAVE THE OUTPUT TO SOME FILE
		counts.saveAsTextFile(outputFilePath);
		
	}

	public static void main(String[] args) {
		
		// Run the java program
		String filePath = args[0];
		String outFilePath = args[1];
		SparkConf conf = new SparkConf().setMaster("local").setAppName("AbhayWordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		System.out.println("Java spark context inited");

		SparkWordCount myCounter = new SparkWordCount();
		myCounter.process(jsc, filePath, outFilePath);
		
	}
}