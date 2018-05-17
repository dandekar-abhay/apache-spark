package org.daa.spark.tests;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class SparkHDFSFileProcessor implements Callable<Integer> {

    private int frequencyTime = 0;

    private SparkContext sparkContext;

    private String stageDirectory;

    private String storageDirectory;

    private FileSystem hdfs;

    AtomicBoolean runningStatus = new AtomicBoolean(false);

    public SparkHDFSFileProcessor(SparkContext sc, FileSystem hdfs, String stageDirectory, String storageDirectory, int frequencyTime){
        this.sparkContext = sc;
        this.stageDirectory = stageDirectory;
        this.frequencyTime = frequencyTime;
        this.storageDirectory = storageDirectory;
        this.hdfs = hdfs;
    }

    @Override
    public Integer call() throws Exception {

        if (runningStatus.get() == true){
            System.out.println("Already running .. returning");
            return -1;
        }else{

            try {
                runningStatus.set(true);
                FileStatus[] fsStatus = hdfs.listStatus(new Path(stageDirectory));

                for (FileStatus status : fsStatus){
                    Path path = status.getPath();
                    System.out.println("Processing File : " + status.getPath());

                    JavaRDD<String> csvDataRDD = sparkContext
                            .textFile(path.toString(), 10)
                            .toJavaRDD();

                    List<String> headerList = new ArrayList<>();
                    headerList.add(csvDataRDD.first());
                    System.out.println("Header : " + headerList.get(0) );

                    JavaRDD sorted = csvDataRDD
                            .filter( line -> line != headerList.get(0))
                            .distinct();

                    JavaSparkContext jsc = new JavaSparkContext(sparkContext);
                    System.out.println("Saving in dir : " + storageDirectory+"/"+status.getPath().getName());
                    jsc.parallelize(headerList).union(sorted).saveAsTextFile(storageDirectory+"/"+status.getPath().getName());
                    return 0;
                }

            } finally {
                runningStatus.set(false);
            }

        }
        return 0;
    }

}
