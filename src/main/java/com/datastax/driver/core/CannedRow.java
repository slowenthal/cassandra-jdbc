package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * DataStax Academy Sample Application
 * <p/>
 * Copyright 2013 DataStax
 */
public class CannedRow implements Row {

  Object[] rowValues;

  public CannedRow(Object... objects) {
    rowValues = objects;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isNull(int i) {
    return rowValues[i] == null;
  }

  @Override
  public boolean isNull(String s) {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean getBool(int i) {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean getBool(String s) {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int getInt(int i) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int getInt(String s) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long getLong(int i) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long getLong(String s) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Date getDate(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Date getDate(String s) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public float getFloat(int i) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public float getFloat(String s) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double getDouble(int i) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double getDouble(String s) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ByteBuffer getBytesUnsafe(String s) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ByteBuffer getBytes(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ByteBuffer getBytes(String s) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public String getString(int i) {
    return rowValues[i].toString();
  }

  @Override
  public String getString(String s) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BigInteger getVarint(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BigInteger getVarint(String s) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BigDecimal getDecimal(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BigDecimal getDecimal(String s) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public UUID getUUID(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public UUID getUUID(String s) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public InetAddress getInet(int i) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public InetAddress getInet(String s) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <T> List<T> getList(int i, Class<T> tClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <T> List<T> getList(String s, Class<T> tClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <T> Set<T> getSet(int i, Class<T> tClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <T> Set<T> getSet(String s, Class<T> tClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <K, V> Map<K, V> getMap(int i, Class<K> kClass, Class<V> vClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <K, V> Map<K, V> getMap(String s, Class<K> kClass, Class<V> vClass) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
