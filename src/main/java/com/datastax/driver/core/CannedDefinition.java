package com.datastax.driver.core;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */
public class CannedDefinition extends ColumnDefinitions.Definition{

  public CannedDefinition(String keyspace, String table, String name, DataType type) {
    super(keyspace, table, name, type);
  }
}
