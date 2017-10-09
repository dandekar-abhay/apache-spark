package apache.spark.poc.db.cache;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import apache.spark.poc.config.Configuration;

public class DBConnection implements Serializable {

  private static final long serialVersionUID = 1918L;

  private boolean isDebug = false;
  static Connection conn = null;
  
  String connString = Configuration.JDBC_DB_URL;
  String DBUser = Configuration.DB_USER;
  String pwd = Configuration.DB_PWD;
  
  final String TABLE_NAME = Configuration.DB_TABLE;
  
  final String INSERT_STMT = "Insert into " + TABLE_NAME + " values ( %s, '%s' )";
  final String UPDATE_STMT = "Update " + TABLE_NAME + " set status = '%s' where task_id = %s";

  
  private void initJDBCConnection(String callee) throws SQLException {
    if (conn == null) {
      System.out.println("Initializing JDBC connection");
      // conn = DriverManager.getConnection(connString, DBUser, pwd);
      conn = setupDataSource(connString, DBUser, pwd).getConnection();
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
    // poolableConnectionFactory.setDefaultQueryTimeout();
    // poolableConnectionFactory.setMaxConnLifetimeMillis(Configuration.DB_QUERY_TIMEOUT);
    poolableConnectionFactory.setDefaultQueryTimeout(Configuration.DB_QUERY_TIMEOUT);
    poolableConnectionFactory.setMaxOpenPrepatedStatements(Configuration.DB_POOL_SIZE);
    
    PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connectionPool);
    return dataSource;
  }

  public static void main(String[] args) throws SQLException {
    DBConnection connection = new DBConnection();
    connection.setStartedStatus(100, "DUMMY_STATUS");
  }  
}
