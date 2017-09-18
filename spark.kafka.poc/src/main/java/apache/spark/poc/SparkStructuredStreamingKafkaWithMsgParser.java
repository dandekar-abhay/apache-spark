package apache.spark.poc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.databind.ObjectMapper;

import apache.spark.poc.config.Configuration;
import apache.spark.poc.entity.Message;

public class SparkStructuredStreamingKafkaWithMsgParser {

  private class StatusClass {
    String hdfsLocation;
    Boolean status;

    public String getHdfsLocation() {
      return hdfsLocation;
    }

    public void setHdfsLocation(String hdfsLocation) {
      this.hdfsLocation = hdfsLocation;
    }

    public Boolean getStatus() {
      return status;
    }

    public void setStatus(Boolean status) {
      this.status = status;
    }

  }

  public static void main(String[] args) throws StreamingQueryException {

    SparkSession spark = SparkSession.builder()
        .appName("JavaStructuredKafkaWordCount").master("local[4]")
        .config("spark.executor.memory", "2g").getOrCreate();

    StructType schemaType =
        new StructType().add("taskId", "long").add("fileName", "string")
            .add("skipHeader", "boolean").add("hdfsLocation", "string");

    // Create DataSet representing the stream of input lines from kafka
    Dataset<String> kafkaValues = spark.readStream().format("kafka")
        .option("kafka.bootstrap.servers", Configuration.KAFKA_BROKER)
        .option("subscribe", Configuration.KAFKA_TOPIC)
        .option("fetchOffset.retryIntervalMs", 100)
        .option("checkpointLocation", "file:///tmp/checkpoint").load()
        .selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

    // StructType schema = kafkaValues.schema();

    Dataset<String> locationRows = kafkaValues.map(x -> {
      ObjectMapper mapper = new ObjectMapper();
      Message m = mapper.readValue(x.getBytes(), Message.class);
      return m.getHdfsLocation();
    }, Encoders.STRING());

    // Start running the query that prints the running counts to the console
    StreamingQuery query = locationRows.writeStream().outputMode("append")
        .option("checkpointLocation", "file:///tmp/checkpoint1")
        .format("console").start();

    // Start a monitoring thread over query
    new Thread(() -> {
      try {
        while (true) {
          System.out.println("Last Progress " + query.lastProgress());
          Thread.sleep(5000);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();

    System.out.println("Spark writing to console :" + query.status());

    query.awaitTermination();
  }
}
