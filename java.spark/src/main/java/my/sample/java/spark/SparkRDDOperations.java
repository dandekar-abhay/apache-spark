package my.sample.java.spark;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRDDOperations {

	public static void main(String[] args) {

		// JavaSparkContext can be used in try-with
		try (JavaSparkContext jsc = new JavaSparkContext(new SparkConf()
				.setMaster("local").setAppName("RDDOps"));) {

			JavaRDD<Integer> ints = jsc.parallelize(Arrays.asList(0, 10, 20, 30));
			JavaRDD<Integer> squares = ints.map(x -> x * x);
			System.out.println("Squares: " + StringUtils.join(squares.collect(), ","));
			JavaRDD<Integer> cubes = ints.map(x -> x * x * x);
			System.out.println("Cubes: " + StringUtils.join(cubes.collect(), ","));
			JavaRDD<Integer> union = squares.union(cubes).distinct();
			System.out.println("Union: " + StringUtils.join(union.collect(), ","));
			JavaRDD<Integer> intersection = squares.intersection(cubes);
			System.out.println("Intersection: " + StringUtils.join(intersection.collect(), ","));
			JavaRDD<Integer> subtraction = squares.subtract(cubes);
			System.out.println("Subtract: " + StringUtils.join(subtraction.collect(), ","));
		}
	}
}
