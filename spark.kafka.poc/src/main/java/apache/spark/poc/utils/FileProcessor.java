package apache.spark.poc.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 * This class will be processing the files Primarily the following operations
 * will be supported 1. Read the file from NFS 2. Process the file for
 * deduplicated ( TBD ) 3. Dump the file into HDFS
 * 
 * @author abhay
 *
 */
public class FileProcessor {

  // TODO : Extract this HDFS specific items to a separate class

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
          .println("Hadoop defaultFs path : " + hdfsConfig.get("fs.defaultFS"));
      hdfs = FileSystem.get(hdfsConfig);
      System.out.println("HDFS initialized successfully");

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /*
   * Reads the location, deduplicates it, dumps it to HDFS
   */
  public static void process(String fileLocation, String hdfsDumpLocation) {

    // Special handle, need to remove this
    if ( fileLocation.startsWith("file:") ) {
      fileLocation = fileLocation.substring(7);
    }
    
    File file = new File(fileLocation);
    
    System.out.println("Path : " + file.getPath());

    // Primary validations
//    if (!file.getAbsoluteFile().exists()) {
//      System.out.println("File " + fileLocation + " does not exist");
//      return;
//    } else 
    if (file.isDirectory()) {
      System.out.println("File " + fileLocation + " is a directory");
      return;
    } else if (file.isFile()) {
      System.out.println("Processing File " + fileLocation);
    }

    try {
      
      Path hdfsFile = new Path(hdfsDumpLocation);
      OutputStream output = hdfs.create(hdfsFile, new Progressable() {
        @Override
        public void progress() {
          System.out.print(".");
        }
      });

      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(output, "UTF-8"));
      
      Stream<String> lines = Files.lines(Paths.get(fileLocation));
      Stream<String> dedupedLines = lines.distinct();
      dedupedLines.forEach( line -> {
        try {
          br.write(line);
          br.newLine();
        } catch (IOException e) {
          System.out.println("IO Exception while writing to HDFS");
        }
      });
      
      lines.close();
      br.flush();
      br.close();
      System.out.println("Successfully processed file : " + fileLocation);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public static void main(String[] args) {
    
    String fileLocation = "file:///home/abhay/MyHome/WorkArea/DataHome/911CallData/911_duplicate.csv";
    String hdfsDumpLocation = "hdfs://localhost:9000/user/data_csv/HDFS-File-Location_911_deduped.csv";
    process(fileLocation, hdfsDumpLocation);
    
  }
  
  
}
