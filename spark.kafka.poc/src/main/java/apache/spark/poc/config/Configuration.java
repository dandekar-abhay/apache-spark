package apache.spark.poc.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class Configuration {
  
  private static Logger logger = Logger.getLogger(Configuration.class);

  final static String KEY_CONFIG_FILE = "CONF_FILE";
  
  /*
   * Spark master URL info
   */
  public static String SPARK_MASTER_URL;
  public static final String KEY_SPARK_MASTER_URL = "spark_master_url";  

  /*
   * Spark master URL info
   */
  public static String SPARK_EXECUTOR_MEMORY;
  public static final String KEY_SPARK_EXECUTOR_MEMORY = "spark_executor_memory";  
  
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
  public static final String KEY_FILE_LIST = "file_list";  

  /*
   * DB URL
   */
  public static String JDBC_DB_URL;
  public static final String KEY_JDBC_DB_URL = "jdbc_url";
  
  /*
   * DB Username
   */
  public static String DB_USER;
  public static final String KEY_DB_USER = "db_user" ;
  
  /*
   * DB Password
   */
  public static String DB_PWD;
  public static final String KEY_DB_PWD = "db_password";
  
  /*
   * DB Table
   */
  public static String DB_TABLE;
  public static final String KEY_DB_TABLE = "status_table";
  
  /*
   * DB Query timeout
   */
  public static int DB_QUERY_TIMEOUT;
  public static final String KEY_DB_QUERY_TIMEOUT = "db_query_timeout_in_millis";
  
  /*
   * MAX DB Connections  
   */
  public static int DB_POOL_SIZE;
  public static final String KEY_DB_POOL_SIZE = "db_pool_size";
  
  /*
   * Checkpoint location
   */
  public static String CHECKPOINT_LOCATION;
  public static final String KEY_CHECKPOINT_LOCATION = "spark_checkpoint_location";
  
  
  static boolean loadProperties(Properties incomingProps) {

    // logger.info(incomingProps);
    
    SPARK_MASTER_URL = incomingProps.getProperty(KEY_SPARK_MASTER_URL, "local[4]");
    SPARK_EXECUTOR_MEMORY = incomingProps.getProperty(KEY_SPARK_EXECUTOR_MEMORY, "2g");
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
    
    JDBC_DB_URL = incomingProps.getProperty(KEY_JDBC_DB_URL, "jdbc:mysql://localhost:3306/db");
    DB_USER = incomingProps.getProperty(KEY_DB_USER, "root");
    DB_PWD = incomingProps.getProperty(KEY_DB_PWD, "");
    DB_TABLE = incomingProps.getProperty(KEY_DB_TABLE, "status_table");
    DB_QUERY_TIMEOUT = Integer.parseInt(incomingProps.getProperty(KEY_DB_QUERY_TIMEOUT, "-1"));
    DB_POOL_SIZE = Integer.parseInt(incomingProps.getProperty(KEY_DB_POOL_SIZE, "8"));
    CHECKPOINT_LOCATION=incomingProps.getProperty(KEY_CHECKPOINT_LOCATION, "file:///tmp/checkpoint");

    logger.info("Using properties as below");
    logger.info("SPARK_MASTER_URL: " + SPARK_MASTER_URL);
    logger.info("SPARK_EXECUTOR_MEMORY: " + SPARK_EXECUTOR_MEMORY);
    logger.info("KAFKA_ZK_QUORUM: " + KAFKA_ZK_QUORUM);
    logger.info("KAFKA_TOPIC: " + KAFKA_TOPIC);
    logger.info("KAFKA_BROKER: " + KAFKA_BROKER);
    logger.info("KAFKA_GROUP_ID: " + KAFKA_GROUP_ID);
    logger.info("KAFKA_PRODUCER_FREQ_SECS: " + KAFKA_PRODUCER_FREQ_SECS);
    logger.info("INPUT_DATA_PATH: " + INPUT_DATA_PATH);
    logger.info("HDFS_URL: " + HDFS_URL);
    logger.info("HDFS_STAGE_DATA_PATH: " + HDFS_STAGE_DATA_PATH);
    logger.info("HDFS_INSTALL_LOCATION: " + HDFS_INSTALL_LOCATION);
    logger.info("FILE_LIST: " + FILE_LIST);
    logger.info("JDBC_DB_URL: " + JDBC_DB_URL);
    logger.info("DB_USER: " + DB_USER);
    logger.info("DB_PWD: " + StringUtils.repeat("*", DB_PWD.length()));
    logger.info("DB_TABLE: " + DB_TABLE);
    logger.info("DB_QUERY_TIMEOUT: " + DB_QUERY_TIMEOUT);
    logger.info("DB_POOL_SIZE: " + DB_POOL_SIZE);
    logger.info("CHECKPOINT_LOCATION: " + CHECKPOINT_LOCATION);
    
    return true;
  }
  
  static {
    try {
      Properties defaultProps = new Properties();
      // possible options for CONF_FILE = conf/dev.properties OR conf/test.properties OR absolute path
      
      String configFile = System.getProperty(KEY_CONFIG_FILE);
      
      if( configFile == null ) {
        logger.info("Found KEY_CONFIG_FILE as null using config file at conf/dev.properties");
        configFile = "conf/dev_local.properties";
      }else {
        logger.info("Using config file from : " + configFile);
      }
      
      FileInputStream in = new FileInputStream(configFile);
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
    Configuration config = new Configuration();
    
    // TEST
    
    logger.info("KAFKA_ZK_QUORUM: " + KAFKA_ZK_QUORUM);
    logger.info("KAFKA_TOPIC: " + KAFKA_TOPIC);
    logger.info("KAFKA_BROKER: " + KAFKA_BROKER);
    logger.info("KAFKA_GROUP_ID: " + KAFKA_GROUP_ID);
    logger.info("KAFKA_PRODUCER_FREQ_SECS: " + KAFKA_PRODUCER_FREQ_SECS);
    logger.info("INPUT_DATA_PATH: " + INPUT_DATA_PATH);
    logger.info("HDFS_URL: " + HDFS_URL);
    logger.info("HDFS_STAGE_DATA_PATH: " + HDFS_STAGE_DATA_PATH);
    logger.info("HDFS_INSTALL_LOCATION: " + HDFS_INSTALL_LOCATION);
    logger.info("FILE_LIST: " + FILE_LIST);

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
