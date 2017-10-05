package apache.spark.poc.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Configuration {

  // public static final String KAFKA_ZK_QUORUM = "10.20.4.35:2181";
  public static final String KAFKA_ZK_QUORUM = "localhost:2181";

  /**
   * Topic name to be referred to
   */
  public static final String KAFKA_TOPIC = "sparktopic_4";

  /**
   * Kafka broker details along with port
   */
  // public static final String KAFKA_BROKER = "10.20.4.35:9092";
  public static final String KAFKA_BROKER = "localhost:9092";

  /**
   * Group ID to be used for kafka
   */
  public static final String KAFKA_GROUP_ID = "";

  public static final int KAFKA_PRODUCER_FREQ_SECS = 10;

  public static final Map<String, String> KAFKA_PROPERTIES =
      new HashMap<String, String>();

//  public static final String INPUT_DATA_PATH =
//      "file:///home/abhay/MyHome/WorkArea/DataHome/search_result";

  public static final String INPUT_DATA_PATH =
       "file:///home/abhay/MyHome/WorkArea/DataHome/911CallData";

  
  public static final String HDFS_URL = "hdfs://localhost:9000";

  public static final String HDFS_STAGE_DATA_PATH = HDFS_URL + "/user/data_csv";

  public static final String HDFS_INSTALL_LOCATION =
      "file:///home/abhay/MyHome/WorkArea/CodeHome/Apache/Hadoop/CDH/hadoop-2.6.0-cdh5.10.0/";

//  public static final List<String> fileList =
//      Arrays.asList(
//          "NCT02406092.xml", "NCT02303444.xml", "NCT02291289.xml",
//          "NCT02220270.xml", "NCT02210819.xml", "NCT02153411.xml",
//          "NCT02141074.xml", "NCT02053779.xml", "NCT02002091.xml",
//          "NCT01991340.xml", "NCT01964391.xml", "NCT01959529.xml",
//          "NCT01917656.xml", "NCT01868477.xml", "NCT01853839.xml",
//          "NCT01833793.xml", "NCT01800006.xml", "NCT01753349.xml",
//          "NCT01720446.xml", "NCT01713530.xml", "NCT01680341.xml",
//          "NCT01649856.xml", "NCT01572038.xml", "NCT01566721.xml",
//          "NCT01536327.xml", "NCT01513590.xml", "NCT01507116.xml",
//          "NCT01503567.xml", "NCT01493778.xml", "NCT01493414.xml",
//          "NCT01491100.xml", "NCT01476423.xml", "NCT01459809.xml",
//          "NCT01459328.xml", "NCT01457456.xml", "NCT01437072.xml",
//          "NCT01435642.xml", "NCT01347580.xml", "NCT01344889.xml",
//          "NCT01331642.xml", "NCT01330277.xml", "NCT01322620.xml",
//          "NCT01306604.xml", "NCT01254188.xml", "NCT01243138.xml",
//          "NCT01241396.xml", "NCT01234545.xml", "NCT01206764.xml",
//          "NCT01204593.xml", "NCT01203111.xml", "NCT01172548.xml",
//          "NCT01068652.xml", "NCT00916097.xml", "NCT00909051.xml",
//          "NCT00869908.xml", "NCT00708344.xml", "NCT00703911.xml",
//          "NCT00700830.xml", "NCT00610272.xml", "NCT00565448.xml",
//          "NCT00283036.xml", "NCT00216333.xml", "NCT00140829.xml");

  public static final List<String> fileList = Arrays.asList("911.csv", "oec.csv");
  
  static {
    // Add all the kafka properties here
    KAFKA_PROPERTIES.put("batch.size", "16384");
    KAFKA_PROPERTIES.put("acks", "all");
    KAFKA_PROPERTIES.put("linger.ms", "1");
    KAFKA_PROPERTIES.put("buffer.memory", "16000");
  }

}
