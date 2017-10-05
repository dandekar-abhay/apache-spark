package apache.spark.poc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.fasterxml.jackson.databind.ObjectMapper;

import apache.spark.poc.config.Configuration;
import apache.spark.poc.entity.Message;
import apache.spark.poc.utils.FileProcessor;

public class SparkSructuredStreamProcessor {

  public static void main(String[] args) throws StreamingQueryException {

    SparkSession spark = SparkSession.builder().appName("StructuredFileReader")
        .master("local[4]").config("spark.executor.memory", "2g").getOrCreate();

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
    
    StreamingQuery query = messages.writeStream().foreach( new ForeachWriter<Message>() {
      
      private static final long serialVersionUID = 1L;
      
      Connection conn = null;
      String connString = "jdbc:mysql://localhost:3306/aera";
      String DBUser = "root";
      String pwd = "Laddu$#712";
      String table_name = "status_table";
      
      String INSERT_STMT = "Insert into "+table_name+" values ( %s, '%s' )" ;
      String UPDATE_STMT = "Update " +table_name+" set status = '%s' where task_id = %s";
      
      private void initJDBCConnection() throws SQLException{
        if ( conn == null ) {
            System.out.println("Initializing JDBC connection");
            conn = DriverManager.getConnection(connString, DBUser, pwd);
        }else {
          System.out.println("Connection is already inited");
        }
      }
      
      private void setStartedStatus (long jobId, String status) throws SQLException {
        initJDBCConnection();
        String insertStmt = String.format(INSERT_STMT, Long.toString(jobId), status);
        Statement stmt = conn.createStatement();
        stmt.execute(insertStmt);
      }

      private void setCompletedStatus (long jobId, String status) throws SQLException {
        System.out.println("Thread id : " + Thread.currentThread().getId());
        initJDBCConnection();
        String updateStmt = String.format(UPDATE_STMT, status, Long.toString(jobId));
        System.out.println("Stmt :-" + updateStmt);
        Statement stmt = conn.createStatement();
        stmt.execute(updateStmt);
        System.out.println("Record updated");
      }
      
      @Override
      public void process(Message message) {
        System.out.println("Entered process method File : " + message.getFileName() );
        if (message.isSkipProcessing()) {
          System.out.println("Received a skip processing signal, skipping processing");
        }else {
          try {
            initJDBCConnection();
            setStartedStatus(message.getTaskId(), "MOVING_TO_HDFS");
            FileProcessor.process(message.getFileName(), message.getHdfsLocation());
            setCompletedStatus(message.getTaskId(), "FINAL_HDFS");
          } catch (SQLException e) {
              System.out.println("Exception while processing job id : " + message.getTaskId());
              e.printStackTrace();
          }
        }
      }
      
      @Override
      public boolean open(long arg0, long arg1) {
        System.out.println("Entered open method params : " + arg0 + " -- " + arg1 );
        return true;
      }
      
      @Override
      public void close(Throwable arg0) {
        
        if (arg0 == null) {
          System.out.println("In close : Throwable arg is null");
        }else {
          System.out.println("In close : Throwable arg is non-null");
        }
        
        System.out.println("Entered close method");
      }
    } ).start();
    
    query.awaitTermination();
  }
}