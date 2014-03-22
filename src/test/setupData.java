package org.apache.cassandra.cql.setup;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */
public class setupData {

  private static final String KEYSPACE = "testks";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception
  {
    Cluster c = Cluster.builder().addContactPoint("localhost").build();
    Session s = c.connect();

    // Drop Keyspace
    String dropKS = String.format("DROP KEYSPACE %s;",KEYSPACE);

    try { stmt.execute(dropKS);}
    catch (Exception e){/* Exception on DROP is OK */}

    // Create KeySpace
    String createKS = String.format("CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",KEYSPACE);
    stmt = con.createStatement();
    stmt.execute(createKS);

    // Use Keyspace
    String useKS = String.format("USE %s;",KEYSPACE);
    stmt.execute(useKS);


    // Create the target Column family
    String create = "CREATE TABLE Test (KEY text PRIMARY KEY, a bigint, b bigint) ;";


    // open it up again to see the new CF
    con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception
  {
    if (con!=null) con.close();
  }


  @Test
  public void test() throws Exception
  {
    String query = "UPDATE Test SET a=?, b=? WHERE KEY=?";
    PreparedStatement statement = con.prepareStatement(query);
    try {
      statement.setLong(1, 100);
      statement.setLong(2, 1000);
      statement.setString(3, "key0");

      statement.executeUpdate();
    }
    finally {
      statement.close();
    }

}
