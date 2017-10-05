package apache.spark.poc.tester;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCTester {

  static Connection conn = null;
  
  static String connString = "jdbc:mysql://localhost:3306/aera"; 
  static String DBUser = "root";
  static String pwd = "Laddu$#712";
  static String table_name = "status_table";
  
  static String INSERT_STMT = "Insert into "+table_name+" values ( %s, '%s' )" ;
  static String UPDATE_STMT = "Update " +table_name+" set status = '%s' where task_id = %s";
  
  private static void initJDBCConnection() throws SQLException{
    if ( conn == null ) {
        System.out.println("Initializing JDBC connection");
        conn = DriverManager.getConnection(connString, DBUser, pwd);
    }
  }
  
  private static void setStartedStatus (long jobId, String status) throws SQLException {
    initJDBCConnection();
    String insertStmt = String.format(INSERT_STMT, Long.toString(jobId), status);
    System.out.println("Stmt :-" + insertStmt);
    Statement stmt = conn.createStatement();
    stmt.execute(insertStmt);
    System.out.println("Record inserted");
  }
  
  private static void setCompletedStatus (long jobId, String status) throws SQLException {
    initJDBCConnection();
    String updateStmt = String.format(UPDATE_STMT, status, Long.toString(jobId));
    System.out.println("Stmt :-" + updateStmt);
    Statement stmt = conn.createStatement();
    stmt.execute(updateStmt);
    System.out.println("Record updated");
  }

  
  public static void main(String[] args) throws SQLException {
    initJDBCConnection();
    setStartedStatus(3, "INITED");
    setCompletedStatus(3, "HDFS_COMPLETE");
  }

}
