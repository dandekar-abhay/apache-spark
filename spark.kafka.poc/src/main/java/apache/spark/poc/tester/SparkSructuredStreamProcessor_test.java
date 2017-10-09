package apache.spark.poc.tester;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.fasterxml.jackson.databind.ObjectMapper;

import apache.spark.poc.config.Configuration;
import apache.spark.poc.entity.Message;

public class SparkSructuredStreamProcessor_test {

  public static void main(String[] args) throws StreamingQueryException {

    SparkSession spark = SparkSession.builder().appName("StructuredFileReader")
        .master("local[4]").config("spark.executor.memory", "2g").getOrCreate();

    // Broadcast<SparkContext> scontextBCast = spark.sparkContext().broadcast(spark.sparkContext(), Encoders.bean(SparkContext.class));
    
    // Create DataSet representing the stream of input lines from kafka
    Dataset<String> kafkaValues = spark.readStream().format("kafka")
        .option("spark.streaming.receiver.writeAheadLog.enable", true)
        .option("kafka.bootstrap.servers", Configuration.KAFKA_BROKER)
        .option("subscribe", Configuration.KAFKA_TOPIC)
        .option("fetchOffset.retryIntervalMs", 100)
        .option("checkpointLocation", "file:///tmp/checkpoint").load()
        .selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

    Dataset<Message> messages = kafkaValues.map(x -> {
      ObjectMapper mapper = new ObjectMapper();
      Message m = mapper.readValue(x.getBytes(), Message.class);
      return m;
    }, Encoders.bean(Message.class));

    // TEST 3 
    // This fails with a Null pointer exception because "spark" is not available on worker node
    
    Dataset<String> statusRDD = messages.map(message -> {

      Dataset<Row> fileDataset = spark.read().option("header", "true").csv(message.getFileName());
      Dataset<Row> dedupedFileDataset = fileDataset.dropDuplicates();
      dedupedFileDataset.rdd().saveAsTextFile(message.getHdfsLocation());
      return message.getHdfsLocation();

    }, Encoders.STRING());
    
  StreamingQuery query2 = statusRDD.writeStream().outputMode("append").format("console").start();

  query2.awaitTermination();

    // messageRDD.foreach( message -> {
    //
    // Dataset<Row> fileDataset = spark.read().option("header",
    // "true").csv(message.getFileName());
    // Dataset<Row> dedupedFileDataset = fileDataset.dropDuplicates();
    // dedupedFileDataset.rdd().saveAsTextFile(message.getHdfsLocation());
    //
    // });

    // messages.map( x -> {
    //
    // String fileLocation = x.
    //
    // }, Encoders.STRING());

    // StreamingQuery query =
    // messages.writeStream().outputMode("append").format("console").start();

  /*
    StreamingQuery query =
        messages.writeStream().foreach(new ForeachWriter<Message>() {

          private static final long serialVersionUID = 1L;

          @Override
          public void process(Message arg0) {
            System.out
                .println("Entered process method File : " + arg0.getFileName());
            FileProcessor.process(arg0.getFileName(), arg0.getHdfsLocation());
          }

          @Override
          public boolean open(long arg0, long arg1) {
            // System.out.println("Entered open method params : " + arg0 + " --
            // " + arg1 );
            return true;
          }

          @Override
          public void close(Throwable arg0) {
            System.out.println("Entered close method");
          }
        }).start();

    // foreach( message -> {
    // Dataset<String> fileRDD = spark.read().option("header",
    // "false").textFile(message.getFileName());
    // fileRDD.dropDuplicates();
    // });

    query.awaitTermination();
    
    */
  }
}