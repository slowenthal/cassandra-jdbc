/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.cql.jdbc;

import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.cql.jdbc.Utils.*;
import static org.apache.cassandra.cql.jdbc.CassandraResultSet.*;

// TODO - Figure out how to make stuff static

/**
 * Implementation class for {@link java.sql.Connection}.
 */
class CassandraConnection extends AbstractConnection implements Connection
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

    public static final int DB_MAJOR_VERSION = 1;
    public static final int DB_MINOR_VERSION = 2;
    public static final String DB_PRODUCT_NAME = "Cassandra";
    public static final String DEFAULT_CQL_VERSION = "3.0.0";
    private final boolean autoCommit = true;

    private final int transactionIsolation = Connection.TRANSACTION_NONE;

    /**
     * Connection Properties
     */
    private Properties connectionProps;

    /**
     * Client Info Properties (currently unused)
     */
    private Properties clientInfo = new Properties();

    /**
     * Set of all Statements that have been created by this connection
     */
    private Set<Statement> statements = new ConcurrentSkipListSet<Statement>();

    protected long timeOfLastFailure = 0;
    protected int numFailures = 0;
    protected String username = null;
    protected String url = null;
    protected String catalog;//current catalog
    protected String currentKeyspace;//current schema
    int majorCqlVersion;

    protected Cluster cluster;
    protected Session session;


    PreparedStatement isAlive = null;
    

    ConsistencyLevel defaultConsistencyLevel;

    /**
     * Instantiates a new CassandraConnection.
     */
    public CassandraConnection(Properties props) throws SQLException
    {
        connectionProps = (Properties)props.clone();
        clientInfo = new Properties();
        url = PROTOCOL + createSubName(props);
        try
        {
            String host = props.getProperty(TAG_SERVER_NAME);
            int port = Integer.parseInt(props.getProperty(TAG_PORT_NUMBER));
            currentKeyspace = props.getProperty(TAG_DATABASE_NAME);
            username = props.getProperty(TAG_USER);
            String password = props.getProperty(TAG_PASSWORD);
            defaultConsistencyLevel = ConsistencyLevel.valueOf(props.getProperty(TAG_CONSISTENCY_LEVEL,ConsistencyLevel.ONE.name()));

    // TODO - process properties
    // TODO - Add properties for load balancing, retry, etc.
    // TODO - do something with the port

            cluster = Cluster.builder()
                    .addContactPoints(host)
                    .build();

           // catalog = cluster.getClusterName();
          catalog = cluster.getMetadata().getClusterName() ;

    // TODO - Add authentication

//            if (username != null)
//            {
//                Map<String, String> credentials = new HashMap<String, String>();
//                credentials.put("username", username);
//                if (password != null) credentials.put("password", password);
//                AuthenticationRequest areq = new AuthenticationRequest(credentials);
//                client.login(areq);
//            }

          //   decoder = new ColumnDecoder(client.describe_keyspaces());

          if (currentKeyspace != null)
            session = cluster.connect(currentKeyspace);
          else
            session = cluster.connect();

          Object[] args = {host, port, currentKeyspace, catalog, defaultConsistencyLevel.name()};
          logger.debug("Connected to {}:{} in Cluster '{}' using Keyspace '{}' and Consistency level {}",args);
        }
        // TODO - fix the exceptions
        catch (Exception e)
        {
            throw new SQLSyntaxErrorException(e);
        }
//        catch (TException e)
//        {
//            throw new SQLNonTransientConnectionException(e);
//        }
//        catch (AuthenticationException e)
//        {
//            throw new SQLInvalidAuthorizationSpecException(e);
//        }
//        catch (AuthorizationException e)
//        {
//            throw new SQLInvalidAuthorizationSpecException(e);
//        }
    }
    
    // get the Major portion of a string like : Major.minor.patch where 2 is the default
    private final int getMajor(String version)
    {
        int major = 0;
        String[] parts = version.split("\\.");
        try
        {
            major = Integer.valueOf(parts[0]);
        }
        catch (Exception e)
        {
            major = 2;
        }
        return major;
    }
    
    private final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
    }

  Session getSession() {
    return session;
  }

  public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    /**
     * On close of connection.
     */
    public synchronized void close() throws SQLException
    {
        // close all statements associated with this connection upon close
        for (Statement statement : statements)
            statement.close();
        statements.clear();
        
        if (isConnected())
        {
            // then disconnect from the transport                
            disconnect();
        }
    }

    public void commit() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public Statement createStatement() throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this);
        statements.add(statement);
        return statement;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency);
        statements.add(statement);
        return statement;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency, resultSetHoldability);
        statements.add(statement);
        return statement;
    }

    public boolean getAutoCommit() throws SQLException
    {
        checkNotClosed();
        return autoCommit;
    }

    public Properties getConnectionProps()
    {
        return connectionProps;
    }

    public String getCatalog() throws SQLException
    {
        checkNotClosed();
        return catalog;
    }

    public void setSchema(String schema) throws SQLException
    {
        checkNotClosed();
        currentKeyspace = schema;
    }

    public String getSchema() throws SQLException
    {
        checkNotClosed();
        return currentKeyspace;
    }

    public Properties getClientInfo() throws SQLException
    {
        checkNotClosed();
        return clientInfo;
    }

    public String getClientInfo(String label) throws SQLException
    {
        checkNotClosed();
        return clientInfo.getProperty(label);
    }

    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are really no commits in Cassandra so no boundary...
        return DEFAULT_HOLDABILITY;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        return new CassandraDatabaseMetaData(this);
    }

    public int getTransactionIsolation() throws SQLException
    {
        checkNotClosed();
        return transactionIsolation;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    public synchronized boolean isClosed() throws SQLException
    {

        return !isConnected();
    }

    public boolean isReadOnly() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    public boolean isValid(int timeout) throws SQLTimeoutException
    {
      // TODO - Vigure this out - this had some kind of timeout checking
      // It looks like it runs some little query too.
        if (timeout < 0) throw new SQLTimeoutException();
        return true ;
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException
    {
        return false;
    }

    public String nativeSQL(String sql) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no distinction between grammars in this implementation...
        // so we are just return the input argument
        return sql;
    }

    public CassandraPreparedStatement prepareStatement(String cql) throws SQLException
    {
        return prepareStatement(cql,DEFAULT_TYPE,DEFAULT_CONCURRENCY,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType) throws SQLException
    {
        return prepareStatement(cql,rsType,DEFAULT_CONCURRENCY,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType, int rsConcurrency) throws SQLException
    {
        return prepareStatement(cql,rsType,rsConcurrency,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType, int rsConcurrency, int rsHoldability) throws SQLException
    {
        checkNotClosed();
        CassandraPreparedStatement statement = new CassandraPreparedStatement(this, cql, rsType,rsConcurrency,rsHoldability);
        statements.add(statement);
        return statement;
    }

    public void rollback() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        checkNotClosed();
        if (!autoCommit) throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setCatalog(String arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no catalog name to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setClientInfo(Properties props) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        if (props != null) clientInfo = props;
    }

    public void setClientInfo(String key, String value) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        clientInfo.setProperty(key, value);
    }

    public void setHoldability(int arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no holdability to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setReadOnly(boolean arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is all connections are read/write in the Cassandra implementation...
        // so we are "silently ignoring" the request
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE) throw new SQLFeatureNotSupportedException(NO_TRANSACTIONS);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    /**
     * Execute a CQL query using the default compression methodology.
     *
     * @param queryStr a CQL query string
     * @param consistencyLevel	the CQL query consistency level
     * @return the query results encoded as a CqlResult structure
     * @throws UnavailableException   when not all required replicas could be created/read
     */
    protected com.datastax.driver.core.ResultSet execute(String queryStr, ConsistencyLevel consistencyLevel)
              throws UnavailableException
    {
      SimpleStatement simpleStatement = new SimpleStatement(queryStr);
      simpleStatement.setConsistencyLevel(consistencyLevel);
      return session.execute(simpleStatement);
    }

    // TODO - implement this
    protected ResultSet execute(com.datastax.driver.core.PreparedStatement ps, Map<Integer, Object> values, ConsistencyLevel consistencyLevel)
    {
      Object[] valarray = new Object[values.size()];
      for (int i = 1; i <= values.size(); i++) {
        valarray[i-1] = values.get(i);
      }
      BoundStatement bs = ps.bind(valarray);
      bs.setConsistencyLevel(consistencyLevel);
      return session.execute(bs);
    }
    
    protected com.datastax.driver.core.PreparedStatement prepare(String queryStr, ConsistencyLevel consistencyLevel) throws Exception
    {
        try
        {
             com.datastax.driver.core.PreparedStatement ps = session.prepare(queryStr);
             ps.setConsistencyLevel(consistencyLevel);
             return ps;
        }
        catch (Exception error)    // TODO - catch correct exceptions
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }

    }
    
    /**
     * Remove a Statement from the Open Statements List
     */
    protected boolean removeStatement(Statement statement)
    {
        return statements.remove(statement);
    }
    
    /**
     * Shutdown the remote connection
     */
    protected void disconnect()
    {
       session.close();
       cluster.close();
    }

    /**
     * Connection state.
     */
    protected boolean isConnected()
    {
        // TODO - do something intelligent here
        return true;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("CassandraConnection [connectionProps=");
        builder.append(connectionProps);
        builder.append("]");
        return builder.toString();
    }    
}
