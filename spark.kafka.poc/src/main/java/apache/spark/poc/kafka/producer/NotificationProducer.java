package apache.spark.poc.kafka.producer;

import java.io.File;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import apache.spark.poc.config.Configuration;
import apache.spark.poc.entity.Message;

public class NotificationProducer {

  private static final boolean debug = true;

  public static void main(String[] argv) throws Exception {

    final String topicName = Configuration.KAFKA_TOPIC;

    // Configure the Producer
    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        Configuration.KAFKA_BROKER);
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    configProperties.put("request.required.acks", "1");

    // configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
    // IntegerPartitioner.class.getCanonicalName());
    // configProperties.put("partitions.0", "0");
    // configProperties.put("partitions.1", "1");
    // configProperties.put("partitions.2", "2");
    // configProperties.put("partitions.3", "3");

    //Timer timer = new Timer();
    TimerTask task = new TimerTask() {

      Message testMessage = new Message();
      ObjectMapper mapper = new ObjectMapper();

      @Override
      public void run() {
        try {
          Producer<String, String> producer =
              new KafkaProducer<String, String>(configProperties);
          
          for (String fname : Configuration.fileList) {
            int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
            String nFSFilePath = Configuration.INPUT_DATA_PATH + File.separator + fname;
            testMessage.setFileName(nFSFilePath);
            testMessage.setSkipHeader(true);
            testMessage.setTaskId(randomNum);
            testMessage.setHdfsLocation(Configuration.HDFS_STAGE_DATA_PATH + "/" + fname);
            String msg = mapper.writeValueAsString(testMessage);
            producer.send(new ProducerRecord<String, String>(topicName, msg));
            if (debug) {
              System.out.println("Message inserted : " + msg);
              System.out.println("Topic : " + topicName);
            }
          }
          producer.close();

        } catch (Exception e) {
          System.err.println("Exception while calling the timer");
          e.printStackTrace(System.err);
        }
      }
    };

   // timer.schedule(task, 1000, Configuration.KAFKA_PRODUCER_FREQ_SECS * 1000);
    System.out.println("Calling run");
//    task.run();

    Message testMessage = new Message();
    ObjectMapper mapper = new ObjectMapper();
    
    try {
      Producer<String, String> producer =
          new KafkaProducer<String, String>(configProperties);
      
      for (String fname : Configuration.fileList) {
        int randomNum = ThreadLocalRandom.current().nextInt(0, 100);
        String nFSFilePath = Configuration.INPUT_DATA_PATH + File.separator + fname;
        testMessage.setFileName(nFSFilePath);
        testMessage.setSkipHeader(true);
        testMessage.setTaskId(randomNum);
        testMessage.setHdfsLocation(Configuration.HDFS_STAGE_DATA_PATH + "/" + fname);
        String msg = mapper.writeValueAsString(testMessage);
        producer.send(new ProducerRecord<String, String>(topicName, msg));
        if (debug) {
          System.out.println("Message inserted : " + msg);
          System.out.println("Topic : " + topicName);
        }
      }
      producer.close();

    } catch (Exception e) {
      System.err.println("Exception while calling the timer");
      e.printStackTrace(System.err);
    }
   
  }
}