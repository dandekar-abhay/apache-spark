package apache.spark.poc;

import java.sql.SQLException;
import java.util.function.Function;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.fasterxml.jackson.databind.ObjectMapper;

import apache.spark.poc.config.Configuration;
import apache.spark.poc.db.cache.DBConnection;
import apache.spark.poc.entity.Message;
import apache.spark.poc.utils.FileProcessor;

public class SparkStructuredStreamProcessor {

    private static Logger logger = Logger.getLogger(SparkStructuredStreamProcessor.class);

    // private static String HDFS_STAGE_LOCATION = "/user/abhay/AERA_HDFS_STAGE";
    public static String HDFS_STAGE_LOCATION = "/user/abhay";

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession.builder().appName("StructuredFileReader")
                .master(Configuration.SPARK_MASTER_URL).config("spark.executor.memory", Configuration.SPARK_EXECUTOR_MEMORY).getOrCreate();

        // Create DataSet representing the stream of input lines from kafka
        Dataset<String> kafkaValues = spark.readStream().format("kafka")
                .option("spark.streaming.receiver.writeAheadLog.enable", true)
                .option("kafka.bootstrap.servers", Configuration.KAFKA_BROKER)
                .option("subscribe", Configuration.KAFKA_TOPIC)
                .option("fetchOffset.retryIntervalMs", 100)
                .load()
                .selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

        Dataset<Message> messages = kafkaValues.map(x -> {
            ObjectMapper mapper = new ObjectMapper();
            Message m = mapper.readValue(x.getBytes(), Message.class);
            return m;
        }, Encoders.bean(Message.class));

        // ====================
        // TEST 1
        // ====================
        // CODE TRYING TO execute MAP on the received RDD
        // This fails with a Null pointer exception because "spark" is not available on worker node

    /*
    Dataset<String> statusRDD = messages.map(message -> {

      Dataset<Row> fileDataset = spark.read().option("header", "true").csv(message.getFileName());
      Dataset<Row> dedupedFileDataset = fileDataset.dropDuplicates();
      dedupedFileDataset.rdd().saveAsTextFile(message.getHdfsLocation());
      return message.getHdfsLocation();

    }, Encoders.STRING());

  StreamingQuery query2 = statusRDD.writeStream().outputMode("append").format("console").start();
  */

        // ====================
        // TEST 2
        // ====================
        // CODE BELOW FAILS WITH EXCEPTION
        // "Queries with streaming sources must be executed with writeStream.start();;"
        // Hence, processing the deduplication on the worker side using
    /*
    JavaRDD<Message> messageRDD = messages.toJavaRDD();

    messageRDD.foreach( message -> {

      Dataset<Row> fileDataset = spark.read().option("header", "true").csv(message.getFileName());
      Dataset<Row> dedupedFileDataset = fileDataset.dropDuplicates();
      dedupedFileDataset.rdd().saveAsTextFile(message.getHdfsLocation());

    });
    */

        // ====================
        // TEST 3
        // ====================
        // CODE TRYING TO COLLECT ALSO FAILS WITH EXCEPTION
        // "Queries with streaming sources must be executed with writeStream.start();;"
        // List<Message> mess = messages.collectAsList();


//    StreamingQuery query =
//        messages.writeStream().outputMode("append").format("console").start();

        // ====================
        // TEST 4
        // ====================
        // Pass the message to workers using a writeStream
        // process each message on workers directly

        StreamingQuery query = messages.writeStream().foreach(new ForeachWriter<Message>() {

            private static final long serialVersionUID = 1L;

            private final DBConnection connection = new DBConnection();

            @Override
            public void process(Message message) {
                logger.info("Process : { File :" + message.getFileName() +
                        ", Thread id:" + Thread.currentThread().getId() +
                        " }"
                );
                if (message.isSkipProcessing()) {
                    logger.info("Received a skip processing signal, skipping processing");
                } else {
                    try {
                        connection.setStatus(message.getTaskId(), "MOVING_TO_HDFS");
                        // int status = FileProcessor.process(message.getFileName(), message.getHdfsLocation());
                        int status = FileProcessor.copyToHDFS(message.getFileName(), HDFS_STAGE_LOCATION);

                        RDD<String> data = spark
                                .sparkContext()
                                .textFile("hdfs://localhost:9000" + HDFS_STAGE_LOCATION + "/" + message.getFileName(), 10);

                        // Check this code, implement it for CSV and then
                        // implement it here
                        // TODO : implement a threaded mechanism for handling if any files are pending cos of failed transactions
                        //
                        String header = data.first();
                        RDD<String> dataWithoutHeader = data.filter(line -> !line.equals(header));
                        dataWithoutHeader.sortBy(new Function<String, String>() {
                            public String call(String arg0) throws Exception {
                                return arg0.split(",")[1];
                            }
                        }, true);

                        // FileProcessor.processFileList(spark.sparkContext(), message.getHdfsLocation());
                        if (status == 0) {
                            connection.setStatus(message.getTaskId(), "FINAL_HDFS");
                        } else {
                            connection.setStatus(message.getTaskId(), "ERROR CODE :" + status);
                        }

                    } catch (SQLException e) {
                        logger.error("Exception while processing job id : " + message.getTaskId());
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public boolean open(long arg0, long arg1) {
                logger.info("Open : Partition Id:" + arg0 + " Version:" + arg1);
                return true;
            }

            @Override
            public void close(Throwable arg0) {

                if (arg0 == null) {
                    logger.info("Close : Throwable arg is null");
                } else {
                    logger.info("Close : Throwable arg is non-null");
                }
            }
        }).option("checkpointLocation", Configuration.CHECKPOINT_LOCATION).start();


        query.awaitTermination();

    }
}