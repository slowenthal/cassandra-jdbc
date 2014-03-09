package com.datastax.driver.core;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.cassandra.cql.jdbc.AbstractJdbcType;
import org.apache.cassandra.cql.jdbc.HandleObjects;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */
public class CannedResultSet implements com.datastax.driver.core.ResultSet {

  private CannedRow[] cannedRows;
  private String[] colNames;
  private int curRowNum;
  private CannedRow curCannedRow;
  private CannedColumnDefinitions columnDefinitions;

  public CannedResultSet() {
    cannedRows = null;
    curRowNum = 0;
    curCannedRow = null;
    columnDefinitions = null;
  }

  public CannedResultSet withRows(CannedRow... cannedRows) {
    if (colNames == null) {
      // We need the column names first
      // TODO - throw something
    }
    this.cannedRows = cannedRows;

    // Populate JDBCTypes off the first row
    if (cannedRows.length > 0) {

      Object[] row1Values = cannedRows[0].rowValues;
      com.datastax.driver.core.ColumnDefinitions.Definition[] defs
              = new com.datastax.driver.core.ColumnDefinitions.Definition[row1Values.length];
      for (int i = 0; i < row1Values.length; i++) {
          DataType dataType;
          if (row1Values[i] == null) {
            dataType = DataType.text();   // TODO - may be good enough
          } else if (row1Values[i].getClass() == int.class) {
            dataType = DataType.cint();
          } else if (row1Values[i].getClass() == boolean.class) {
            dataType = DataType.cboolean();
          } else {  // May be good enough.  Most things are text
            // TODO Build this out
            dataType = DataType.text();
          }
          defs[i] = new CannedDefinition("","",colNames[i], dataType);     // TODO - What goes in the empty quotes
      }
      columnDefinitions = new CannedColumnDefinitions(defs);
    }

    return this;
  }


  public CannedResultSet withColNames(String...colNames) {
    this.colNames = colNames;
    return this;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefinitions;
  }

  @Override
  public Row one() {
    if (curRowNum < cannedRows.length) {
      curRowNum++;
      curCannedRow = cannedRows[curRowNum - 1];
      return curCannedRow;
    } else {
      // TODO - do we throw something here?
      return null;
    }
  }

  @Override
  public List<Row> all() {
    return Arrays.asList((Row[]) cannedRows);
  }

  @Override
  public Iterator<Row> iterator() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int getAvailableWithoutFetching() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isFullyFetched() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ListenableFuture<Void> fetchMoreResults() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<ExecutionInfo> getAllExecutionInfo() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isExhausted() {
    return curRowNum >= cannedRows.length;
  }

}

