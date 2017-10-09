package apache.spark.poc.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Configuration_File {

  /*
   * Zookeeper info
   */
  public static String KAFKA_ZK_QUORUM;
  public static final String KEY_KAFKA_ZK_QUORUM = "kafka_zk_quorum";
  
  /*
   * Topic name to be referred to
   */
  public static String KAFKA_TOPIC;
  public static final String KEY_KAFKA_TOPIC = "kafka_topic";

  /*
   * Kafka broker details along with port
   */
  public static String KAFKA_BROKER; 
  public static final String KEY_KAFKA_BROKER = "kafka_broker";
  
  /*
   *Kafka group id 
   */
  public static String KAFKA_GROUP_ID;
  public static final String KEY_KAFKA_GROUP_ID = "kafka_group_id";
  
  /*
   * Frequency for message generation for testing
   */
  public static int KAFKA_PRODUCER_FREQ_SECS;
  public static final String KEY_KAFKA_PRODUCER_FREQ_SECS = "kafka_producer_freq_secs";

  /*
   * Data input location
   */
  public static String INPUT_DATA_PATH;
  public static final String KEY_INPUT_DATA_PATH = "input_data_path";

  /*
   * HDFS URL
   */
  public static String HDFS_URL;
  public static final String KEY_HDFS_URL = "hdfs_url";

  /*
   * HDFS stage path
   */
  public static String HDFS_STAGE_DATA_PATH = HDFS_URL;
  public static final String KEY_HDFS_STAGE_DATA_PATH = "hdfs_stage_data_path";

  /*
   * HDFS INSTALL LOCATION, required to pick up the default HDFS properties
   */
  public static String HDFS_INSTALL_LOCATION;
  public static final String KEY_HDFS_INSTALL_LOCATION = "hdfs_install_location";

  /*
   * Files to process
   */
  public static List<String> FILE_LIST = new ArrayList<>();
//      Arrays.asList("911.csv", "oec.csv", "911_1.csv", "oec_1.csv",
//          "12MB_fromGz.csv", "12MB_fromGz_1.csv");
  public static final String KEY_FILE_LIST = "file_list";  
  
  static boolean loadProperties(Properties incomingProps) {

    System.out.println("Using properties as below");
    System.out.println(incomingProps);
    
    KAFKA_ZK_QUORUM = incomingProps.getProperty(KEY_KAFKA_ZK_QUORUM, "localhost:2181");
    KAFKA_TOPIC = incomingProps.getProperty(KEY_KAFKA_TOPIC, "sparktopic_4");
    KAFKA_BROKER = incomingProps.getProperty(KEY_KAFKA_BROKER, "localhost:9092");
    KAFKA_GROUP_ID = incomingProps.getProperty(KEY_KAFKA_GROUP_ID, "");
    KAFKA_PRODUCER_FREQ_SECS = Integer.parseInt(incomingProps.getProperty(KEY_KAFKA_PRODUCER_FREQ_SECS, "20"));
    INPUT_DATA_PATH = incomingProps.getProperty(KEY_INPUT_DATA_PATH, "file:///home/abhay/MyHome/WorkArea/DataHome/911CallData");
    HDFS_URL = incomingProps.getProperty(KEY_HDFS_URL, "hdfs://localhost:9000");
    HDFS_STAGE_DATA_PATH = incomingProps.getProperty(KEY_HDFS_STAGE_DATA_PATH, "/user/data_csv");    
    HDFS_INSTALL_LOCATION = incomingProps.getProperty(KEY_HDFS_INSTALL_LOCATION, "file:///home/abhay/MyHome/WorkArea/CodeHome/Apache/Hadoop/CDH/hadoop-2.6.0-cdh5.10.0/");
    FILE_LIST = Arrays.asList(incomingProps.getProperty(KEY_FILE_LIST).replaceAll(" ", "").split(","));
    
    return true;
  }
  
  static {
    try {
      Properties defaultProps = new Properties();
      FileInputStream in = new FileInputStream("conf/dev.properties");
      defaultProps.load(in);
      in.close();
      
      loadProperties(defaultProps);
     
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Configuration_File config = new Configuration_File();
    
    // TEST
    
    System.out.println("KAFKA_ZK_QUORUM: " + KAFKA_ZK_QUORUM);
    System.out.println("KAFKA_TOPIC: " + KAFKA_TOPIC);
    System.out.println("KAFKA_BROKER: " + KAFKA_BROKER);
    System.out.println("KAFKA_GROUP_ID: " + KAFKA_GROUP_ID);
    System.out.println("KAFKA_PRODUCER_FREQ_SECS: " + KAFKA_PRODUCER_FREQ_SECS);
    System.out.println("INPUT_DATA_PATH: " + INPUT_DATA_PATH);
    System.out.println("HDFS_URL: " + HDFS_URL);
    System.out.println("HDFS_STAGE_DATA_PATH: " + HDFS_STAGE_DATA_PATH);
    System.out.println("HDFS_INSTALL_LOCATION: " + HDFS_INSTALL_LOCATION);
    System.out.println("FILE_LIST: " + FILE_LIST);
    
  }
  
}
