package apache.spark.poc.db.cache;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class DBConnection implements Serializable {

  private static final long serialVersionUID = 1918L;

  private boolean isDebug = false;
  static Connection conn = null;
  
  String connString = "jdbc:mysql://localhost:3306/aera";
  String DBUser = "root";
  String pwd = "Laddu$#712";
  
  final String TABLE_NAME = "status_table";
  final String INSERT_STMT = "Insert into " + TABLE_NAME + " values ( %s, '%s' )";
  final String UPDATE_STMT = "Update " + TABLE_NAME + " set status = '%s' where task_id = %s";

  
  private void initJDBCConnection(String callee) throws SQLException {
    if (conn == null) {
      System.out.println("Initializing JDBC connection");
      // conn = DriverManager.getConnection(connString, DBUser, pwd);
      conn = setupDataSource("jdbc:mysql://localhost:3306/aera", "root", "Laddu$#712").getConnection();
    } else {
      System.out.println(callee + " : Connection is already inited");
    }
  }

  public void setStartedStatus(long jobId, String status) throws SQLException {
    initJDBCConnection("setStartedStatus");
    String insertStmt =
        String.format(INSERT_STMT, Long.toString(jobId), status);
    Statement stmt = conn.createStatement();
    stmt.execute(insertStmt);
  }

  public void setCompletedStatus(long jobId, String status)
      throws SQLException {
    initJDBCConnection("setCompletedStatus");
    String updateStmt =
        String.format(UPDATE_STMT, status, Long.toString(jobId));
    if (isDebug) {
      System.out.println("Stmt :-" + updateStmt);
    }
    Statement stmt = conn.createStatement();
    stmt.execute(updateStmt);
    if (isDebug) {
      System.out.println("Record updated");
    }
  }

  public static DataSource setupDataSource(String connectURI, String username, String password) {

    ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectURI, username, password);
    PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
    ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);
    
    PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connectionPool);
    return dataSource;
  }

  public static void main(String[] args) throws SQLException {
    DBConnection connection = new DBConnection();
    connection.setStartedStatus(100, "DUMMY_STATUS");
  }  
}
