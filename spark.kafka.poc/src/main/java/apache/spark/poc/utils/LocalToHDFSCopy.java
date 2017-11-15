package apache.spark.poc.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class LocalToHDFSCopy {

  private static FileSystem hdfs;

  private static Logger logger = Logger.getLogger(LocalToHDFSCopy.class);

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

      logger.info("Hadoop conf path : " + hdfsConfig.get("fs.default.name"));
      logger.info("Hadoop conf path : " + hdfsConfig.get("fs.defaultFS"));
      hdfs = FileSystem.get(hdfsConfig);
      logger.info("HDFS initialized successfully");

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

    logger.info(
        "Received a copy request : \n\t " + source + "\n TO : \n\t" + dest);

    Path hdfsPath =
        new Path(apache.spark.poc.config.Configuration.HDFS_STAGE_DATA_PATH);

    try {
      if (!hdfs.exists(hdfsPath)) {
        logger.info("Destination HDFS directory does not exist : " + hdfsPath);
        hdfs.mkdirs(hdfsPath);
        logger.info("Destination HDFS dir created successfully");
      }
      Path src = new Path(source);
      Path dst = new Path(dest);

      logger.info("Copy initiated from : " + src + " to : " + dst);
      hdfs.copyFromLocalFile(src, dst);
      logger.info("Copy completed successfully");

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