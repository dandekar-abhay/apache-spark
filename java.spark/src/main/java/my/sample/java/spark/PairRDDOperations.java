package my.sample.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PairRDDOperations {

	JavaSparkContext jsc;

	public PairRDDOperations() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("AbhayWordCount");
		this.jsc = new JavaSparkContext(conf);
		System.out.println("Java spark context inited");
	}

	public static void main(String[] args) {
		
		String inputFilePath = "/tmp/README.md";
		String outputFilePath = "/tmp/output";
		
		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String x) {
				return new Tuple2<String, String>(x.split(" ")[0], x);
			}
		};

		//PairFunction<String, String, String> data = new PairFunction<String, String, String>(x -> new Tuple2(x.split(" ")[0],x) ) ;
		
		PairRDDOperations pairRDDOps = new PairRDDOperations();
		
		JavaRDD<String> text = pairRDDOps.jsc.textFile(inputFilePath);
		JavaPairRDD<String, String> pairs = text.mapToPair(x -> new Tuple2<String, String>(x.split(" ")[0],x));

		for (Tuple2<String, String> tuple : pairs.collect()) {		
			System.out.println(tuple._1 + " => " + tuple._2);
		}
		
	}

}
