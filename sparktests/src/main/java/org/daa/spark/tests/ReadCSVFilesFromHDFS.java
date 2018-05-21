package org.daa.spark.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class ReadCSVFilesFromHDFS {

    public static void processFile(SparkContext sparkContext, String fileName){

        JavaRDD<String> csvDataRDD = sparkContext
                .textFile(fileName, 10)
                .toJavaRDD();

        // Note : if data size > 1 GB then load and repartition

        System.out.println("Total partitions : " + csvDataRDD.partitions().size());
        System.out.println("RDD length : " + csvDataRDD.count());

        String header = csvDataRDD.first();
        System.out.println("Header : " + header );
//        RDD<String> dataWithoutHeader = csvDataRDD.filter( line -> {
//                    System.out.println(line.getClass());
//                    return true;
//                }
//            //!(line.equals(header))
//            );

        JavaRDD distinct = csvDataRDD.distinct();
        distinct.filter( line -> line != header  );
        System.out.println("New first : " + distinct.first());
        System.out.println("Distinct count : " + distinct.count());

        JavaRDD sorted = distinct.sortBy( line -> line, true, 2 );
        System.out.println("Sorted count : " + sorted.count() );
    }

    public static void startThread(){

        Runnable runner = new Runnable(){

            @Override
            public void run() {



            }
        };

    }



    public static void main(String[] args) {



        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkCVSReader")
                .master("local[2]")
                .getOrCreate();

        processFile(sparkSession.sparkContext(), "hdfs://localhost:9000/user/abhay/HDFS_STAGE/1000Lines_withDuplicates");

    }
}
