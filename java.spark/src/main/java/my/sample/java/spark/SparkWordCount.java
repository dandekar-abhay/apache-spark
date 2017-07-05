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

/**
 * This is the code to split the data into tokens
 * @author abhay
 */
class SparkDataSplitter implements FlatMapFunction<String, String> { 

	private static final long serialVersionUID = -7255813783532436585L;

	public Iterator<String> call(String oneLine) throws Exception {
		return Arrays.asList(oneLine.split(" ")).iterator();
	}

}
/**
 * This is he mapper, converting the tokens to Key, value pair
 * @author abhay
 *
 */
class SparkMapper implements PairFunction<String, String, Integer> { 
	
	private static final long serialVersionUID = 1L;

	public Tuple2<String, Integer> call(String x) throws Exception {
		return new Tuple2<String, Integer>(x, 1);
	}
}

/**
 * This is a reducer, aggregating over the tokens 
 * @author abhay
 *
 */
class SparkReducer implements Function2<Integer, Integer, Integer> {

	private static final long serialVersionUID = -8026017422209732439L;

	public Integer call(Integer arg0, Integer arg1) throws Exception {
		return arg0 + arg1;
	}
}

public class SparkWordCount { // implements Serializable {

	// WE NEED TO MAKE THIS SERIALIZABLE AS THIS CLASS WILL BE PASSED OVER
	// THE WIRE IF WE DEFINE ALL THE EXECUTABLE CODE HERE IN DRIVER CLASS
	// check processInternal, it needs a serialVersionID defined here
	//
	// If we move the RDD code out of this driver class, then we need not serialize it
	
	// private static final long serialVersionUID = 7040262147300391556L;
	
	JavaSparkContext jsc;
	
	public SparkWordCount() {
		// TODO Auto-generated constructor stub
		SparkConf conf = new SparkConf().setMaster("local").setAppName("AbhayWordCount");
		this.jsc = new JavaSparkContext(conf);
		System.out.println("Java spark context inited");
	}

	public void processExternal(JavaSparkContext jsc, String inputFilePath, String outputFilePath) {
		JavaRDD<String> text = jsc.textFile(inputFilePath);

		JavaRDD<String> words = text.flatMap(new SparkDataSplitter());

		long totalCount = words.count();
		System.out.println("Total words : " + totalCount);

		JavaPairRDD<String, Integer> counts = words.mapToPair(new SparkMapper()).reduceByKey(new SparkReducer());

		// SAVE THE OUTPUT TO SOME FILE
		counts.saveAsTextFile(outputFilePath);

	}
	
	public void processWithLambdas(JavaSparkContext jsc, String inputFilePath, String outputFilePath) {
		JavaRDD<String> text = jsc.textFile(inputFilePath);

		JavaRDD<String> words = text.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

		long totalCount = words.count();
		System.out.println("Total words : " + totalCount);

		JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w,1) ).reduceByKey((x,y) -> x+y);

		// SAVE THE OUTPUT TO SOME FILE
		counts.saveAsTextFile(outputFilePath);

	}

	public void processInternal(JavaSparkContext jsc, String inputFilePath, String outputFilePath) {
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

					public Tuple2<String, Integer> call(String x) {
						// SEND ALL THE STRINGS WITH 1 AS VALUE
						// TUPLE2 IS A SCALA CLASS, PROVIDED BY SPARK
						return new Tuple2<String, Integer>(x, 1);
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
//		String inputFilePath = args[0];
//		String outputFilePath = args[1];
		
		String inputFilePath = "/tmp/README.md";
		String outputFilePath = "/tmp/output";

		// Moved sparkcontext to class level variable
//		SparkConf conf = new SparkConf().setMaster("local").setAppName("AbhayWordCount");
//		JavaSparkContext jsc = new JavaSparkContext(conf);
//		System.out.println("Java spark context inited");

		SparkWordCount myCounter = new SparkWordCount();
		// myCounter.processInternal(jsc, inputFilePath, outputFilePath);
		// myCounter.processExternal(myCounter.jsc, inputFilePath, outputFilePath);
		myCounter.processWithLambdas(myCounter.jsc, inputFilePath, outputFilePath);
	}
}