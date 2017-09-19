package apache.spark.poc.utils;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class LocalToHDFSCopy {

  private static FileSystem hdfs;

  static {

    try {

      String HDFS_INSTALL_LOCATION =
          apache.spark.poc.config.Configuration.HDFS_INSTALL_LOCATION;

      Configuration hdfsConfig = new Configuration();
      hdfsConfig.addResource(
          new Path(HDFS_INSTALL_LOCATION + "/etc/hadoop/core-site.xml"));
      hdfsConfig.addResource(
          new Path(HDFS_INSTALL_LOCATION + "/etc/hadoop/hdfs-site.xml"));
      hdfsConfig.addResource(
          new Path(HDFS_INSTALL_LOCATION + "/etc/hadoop/mapred-site.xml"));

      System.out
          .println("Hadoop conf path : " + hdfsConfig.get("fs.default.name"));
      System.out
          .println("Hadoop conf path : " + hdfsConfig.get("fs.defaultFS"));
      hdfs = FileSystem.get(hdfsConfig);
      System.out.println("HDFS initialized successfully");

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Copies a file from Local to HDFS
   * 
   * @param source
   * @param dest
   * @return
   */

  public static boolean copyToHDFS(String source, String dest) {

    System.out.println(
        "Received a copy request : \n\t " + source + "\n TO : \n\t" + dest);

    Path hdfsPath =
        new Path(apache.spark.poc.config.Configuration.HDFS_STAGE_DATA_PATH);

    try {
      if (!hdfs.exists(hdfsPath)) {
        System.out
            .println("Destination HDFS directory does not exist : " + hdfsPath);
        hdfs.mkdirs(hdfsPath);
        System.out.println("Destination HDFS dir created successfully");
      }
      Path src = new Path(source);
      Path dst = new Path(dest);

      System.out.println("Copy initiated from : " + src + " to : " + dst);
      hdfs.copyFromLocalFile(src, dst);
      System.out.println("Copy completed successfully");
      
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public static void main(String[] args) {
    String source = apache.spark.poc.config.Configuration.INPUT_DATA_PATH
        + "/NCT02406092.xml";
    String dest = apache.spark.poc.config.Configuration.HDFS_STAGE_DATA_PATH
        + "/NCT02406092_1.xml";
    copyToHDFS(source, dest);
  }

}