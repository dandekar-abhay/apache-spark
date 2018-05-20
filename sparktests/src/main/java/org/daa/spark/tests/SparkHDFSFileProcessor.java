package org.daa.spark.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
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
        this.frequencyTime = frequencyTime;
    }

    @Override
    public Integer call() throws Exception {

        String baseHDFSUrl = hdfs.getConf().get("fs.defaultFS");

        if (runningStatus.get() == true){
            System.out.println("Already running .. returning");
            return -1;
        }else{

            try {
                runningStatus.set(true);
                FileStatus[] fsStatus = hdfs.listStatus(new Path(stageDirectory));

                for (FileStatus status : fsStatus){
                    System.out.println("Files found : " + status.getPath());
                }

                for (FileStatus status : fsStatus){
                    Path path = status.getPath();
                    System.out.println("Processing File : " + status.getPath());

                    JavaRDD<String> csvDataRDD = sparkContext
                            .textFile(path.toString(), 10)
                            .toJavaRDD();

                    List<String> headerList = new ArrayList<>();
                    headerList.add(csvDataRDD.first());
                    // System.out.println("Header : " + headerList.get(0) );

                    JavaRDD sorted = csvDataRDD
                            .filter( line -> line != headerList.get(0))
                            .distinct();

                    JavaSparkContext jsc = new JavaSparkContext(sparkContext);

                    //System.out.println("Saving in dir : " + baseHDFSUrl +storageDirectory+"/"+status.getPath().getName());
                    jsc
                            .parallelize(headerList)
                            .union(sorted)
                            .coalesce(1)
                            .saveAsTextFile( baseHDFSUrl + storageDirectory+"_temp/"+status.getPath().getName());

                    hdfs.rename(
                            new Path(baseHDFSUrl + storageDirectory+"_temp/"+status.getPath().getName()+"/part-00000"),
                            new Path(baseHDFSUrl + storageDirectory+"/"+status.getPath().getName())
                    );

                    // delete the temp dir
                    hdfs.delete(new Path(baseHDFSUrl + storageDirectory+"_temp/"+status.getPath().getName()), true);
                    // delete the file in stage
                    hdfs.delete(status.getPath(), true);
                }
                return 0;

            } finally {
                runningStatus.set(false);
            }
        }
    }


    public static void main(String[] args) {

        Timer timer = new Timer();

        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkCVSReader")
                .master("local[2]")
                .getOrCreate();

        FileSystem hdfs = null;

        try {

            String HDFS_INSTALL_LOCATION = "/home/abhay/MyHome/WorkHome/CodeHome/Apache/Hadoop/hadoop-2.7.5";
            System.out.println("Using HDFS location : " + HDFS_INSTALL_LOCATION);

            Configuration hdfsConfig = new Configuration();
            hdfsConfig.addResource(
                    new Path(HDFS_INSTALL_LOCATION + "/etc/hadoop/core-site.xml"));
            hdfsConfig.addResource(
                    new Path(HDFS_INSTALL_LOCATION + "/etc/hadoop/hdfs-site.xml"));
            hdfsConfig.addResource(
                    new Path(HDFS_INSTALL_LOCATION + "/etc/hadoop/mapred-site.xml"));

            hdfs = FileSystem.get(hdfsConfig);
            System.out.println("Base URL : " + hdfs.getConf().get("fs.defaultFS"));

            System.out.println("HDFS initialized successfully");

            SparkHDFSFileProcessor fileProcessor = new SparkHDFSFileProcessor(
                    sparkSession.sparkContext(), hdfs,
                    "/user/abhay/AERA_HDFS_STAGE", "/user/abhay/AERA_HDFS", 1 );

                    try {
                        fileProcessor.call();
                    } catch (Exception e) {
                        System.err.println("Exception while calling the timer");
                        e.printStackTrace(System.err);
                    }

//            TimerTask task = new TimerTask () {
//                @Override
//                public void run() {
//                    try {
//                        fileProcessor.call();
//                    } catch (Exception e) {
//                        System.err.println("Exception while calling the timer");
//                        e.printStackTrace(System.err);
//                    }
//                }
//            };
//
//            // 1 sec tester
//            // timer.schedule(task, 1000, 1000 );
//            timer.schedule(task, 1000, (1 * 60 * 1000) / 6);

        } catch (IOException e) {
            System.out.println("Could not init HDFS : " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }




    }
}
