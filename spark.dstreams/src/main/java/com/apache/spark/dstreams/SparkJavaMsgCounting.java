package com.apache.spark.dstreams;


import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.util.*;

/**
 * Created by datum on 30/8/17.
 */
public class SparkJavaMsgCounting {

    public static void main(String[] args) throws Exception {
        int streamInterval = 10; //milliSec
        //System.setProperty("spark.executor.memory", "8g");
        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //System.setProperty("spark.kryo.registrator", "com.datatorrent.benchmarking.spark.streaming.adsdimension.Application.MyKryoRegistrator");

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("Wordcount");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext javaStreamingContext =
                new JavaStreamingContext(javaSparkContext, new Duration(3000));

        Set<String> topics = new HashSet<String>();
        topics.add("sparktopic");

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        //kafkaParams.put("group.id", "wordcount-group");
        kafkaParams.put("auto.offset.reset", "false");
        kafkaParams.put("enable.auto.commit", "false");

        JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(javaStreamingContext,
                        String.class, String.class,
                        StringDecoder.class, StringDecoder.class,
                        kafkaParams, topics);

        JavaPairDStream<String, String> messagesDStream = messages.transformToPair((pairRdd) ->  pairRdd);

        messagesDStream.foreachRDD((pairRdd) -> {

            OffsetRange[] offsetRanges = ((HasOffsetRanges) pairRdd.rdd()).offsetRanges();
            for (OffsetRange offsetRange: offsetRanges) {
                System.out.println("Topic: " + offsetRange.topic()
                        + " Partition: " + offsetRange.partition()
                        + " From offset: " + offsetRange.fromOffset()
                        + " Until offset: " + offsetRange.untilOffset());
            }

            JavaRDD lines = pairRdd.map((tuple2) -> tuple2._2());

            JavaRDD words = lines.flatMap((line) -> {
                    String[] wordArr = ((String)line).split("\\s");
                    return Arrays.asList(wordArr);
            });

            JavaPairRDD wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {

                public Tuple2<String, Integer> call(String word) throws Exception {
                    return new Tuple2<String, Integer>(word, 1);
                }
            });

            JavaPairRDD reducedWordCounts = wordCounts.reduceByKey(new Function2<Integer, Integer, Integer>() {

                public Integer call(Integer i1, Integer i2) throws Exception {
                    return i1 + i2;
                }
            });

            List results = reducedWordCounts.collect();
            if (results.size() > 0) {
                System.out.println("Reduced word counts: " + results.get(0));
            }

            return null;
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
