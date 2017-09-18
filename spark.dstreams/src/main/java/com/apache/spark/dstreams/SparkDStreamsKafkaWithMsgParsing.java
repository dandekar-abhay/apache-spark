package com.apache.spark.dstreams;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.fasterxml.jackson.databind.ObjectMapper;

import apache.spark.poc.entity.Message;
import apache.spark.poc.utils.LocalToHDFSCopy;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkDStreamsKafkaWithMsgParsing {

  public static void main(String[] args) throws Exception {
    int streamInterval = 10; // milliSec
    // System.setProperty("spark.executor.memory", "8g");
    System.setProperty("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer");
    // System.setProperty("spark.kryo.registrator",
    // "com.datatorrent.benchmarking.spark.streaming.adsdimension.Application.MyKryoRegistrator");

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster("local[4]");
    sparkConf.setAppName("KafkaReadDStreams");
    
    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
    JavaStreamingContext javaStreamingContext =
        new JavaStreamingContext(javaSparkContext, new Duration(3000));

    Set<String> topics = new HashSet<String>();
    topics.add("sparktopic");

    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", "localhost:9092");
    //kafkaParams.put("auto.offset.reset", "false");
    //kafkaParams.put("enable.auto.commit", "false");

    JavaPairInputDStream<String, String> messages = KafkaUtils
        .createDirectStream(javaStreamingContext, String.class, String.class,
            StringDecoder.class, StringDecoder.class, kafkaParams, topics);

    JavaPairDStream<String, String> messagesDStream =
        messages.transformToPair((pairRdd) -> pairRdd);

    messagesDStream.foreachRDD((pairRdd) -> {

      OffsetRange[] offsetRanges =
          ((HasOffsetRanges) pairRdd.rdd()).offsetRanges();
      for (OffsetRange offsetRange : offsetRanges) {
        System.out.println("Kafka Topic: " + offsetRange.topic() + " Partition:- "
            + offsetRange.partition() + " OFFSET FROM: "
            + offsetRange.fromOffset() + " OFFSET UNTIL: "
            + offsetRange.untilOffset());
      }

      JavaRDD<String> lines = pairRdd.map((tuple2) -> tuple2._2());

      JavaRDD<Message> kafkaMessage = lines.map(x -> {
        ObjectMapper mapper = new ObjectMapper();
        Message m = mapper.readValue(x.getBytes(), Message.class);
        return m;
      });

      JavaPairRDD<String, Boolean> copyHDFSStatus =
          kafkaMessage.mapToPair(message -> {
            System.out
                .println("Copying file from " + message.getHdfsLocation());
            boolean copyStatus = LocalToHDFSCopy.copyToHDFS(message.getFileName(), message.getHdfsLocation());
            return new Tuple2<String, Boolean>(message.getHdfsLocation(), copyStatus);
          });

      List<Tuple2<String, Boolean>> results = copyHDFSStatus.collect();

      if (results.size() > 0) {
        System.out.println("Copied files counts: " + results.get(0));
      }
      return null;
    });

    javaStreamingContext.start();
    javaStreamingContext.awaitTermination();
  }

}
