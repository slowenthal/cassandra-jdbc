package com.datastax.driver.core;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */

// Dirty - to expose the public constructor

public class CannedColumnDefinitions extends ColumnDefinitions {

  public CannedColumnDefinitions(Definition[] defs) {
    super(defs);
  }
}
