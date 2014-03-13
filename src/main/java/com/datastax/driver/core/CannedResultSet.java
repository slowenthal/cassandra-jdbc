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

  private List<Row> cannedRows;
  private int curRowNum;
  private CannedRow curCannedRow;
  private CannedColumnDefinitions columnDefinitions;


  public CannedResultSet(Object...objectPairs) {
    // Format of the parameters is name, type, name, type, ...
    // Breaks some rules, but so what?

    cannedRows = null;
    curRowNum = 0;
    curCannedRow = null;
    columnDefinitions = null;
    cannedRows = new ArrayList<Row>();


    int numElements = objectPairs.length / 2;
    com.datastax.driver.core.ColumnDefinitions.Definition[] defs
            = new com.datastax.driver.core.ColumnDefinitions.Definition[numElements];

    for (int i = 0; i < numElements; i++) {
        defs[i] = new CannedDefinition("","",  (String)objectPairs[i * 2], (DataType) objectPairs[i * 2 + 1]);
    }
    this.columnDefinitions = new CannedColumnDefinitions(defs);
  }

  public CannedResultSet addRow(Object... objects) {
     this.cannedRows.add(new CannedRow(objects));
    return this;
  }

  public CannedResultSet sortAlpha(final int...keys) {

    Collections.sort(this.cannedRows,new Comparator<Row>() {
      @Override
      public int compare(Row row, Row row2) {
        int result = 0;
        for(int k = 0; k < keys.length && result == 0; k++) {
           result = row.getString(keys[k]).compareTo(row2.getString(keys[k]));
        }
        return result;
      }
    } );

    return this;
  }


  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefinitions;
  }

  @Override
  public Row one() {
    if (curRowNum < cannedRows.size()) {
      curRowNum++;
      curCannedRow = (CannedRow)cannedRows.get(curRowNum - 1);
      return curCannedRow;
    } else {
      // TODO - do we throw something here?
      return null;
    }
  }

  @Override
  public List<Row> all() {
    return cannedRows;
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
    return curRowNum >= cannedRows.size();
  }

}

