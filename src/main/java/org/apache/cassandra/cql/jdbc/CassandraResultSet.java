/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.cql.jdbc;

import static org.apache.cassandra.cql.jdbc.Utils.*;
import static org.apache.cassandra.utils.ByteBufferUtil.string;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * <p>
 * The Supported Data types in CQL are as follows:
 * </p>
 * <table>
 * <tr>
 * <th>type</th>
 * <th>java type</th>
 * <th>description</th>
 * </tr>
 * <tr>
 * <td>ascii</td>
 * <td>String</td>
 * <td>ASCII character string</td>
 * </tr>
 * <tr>
 * <td>bigint</td>
 * <td>Long</td>
 * <td>64-bit signed long</td>
 * </tr>
 * <tr>
 * <td>blob</td>
 * <td>ByteBuffer</td>
 * <td>Arbitrary bytes (no validation)</td>
 * </tr>
 * <tr>
 * <td>boolean</td>
 * <td>Boolean</td>
 * <td>true or false</td>
 * </tr>
 * <tr>
 * <td>counter</td>
 * <td>Long</td>
 * <td>Counter column (64-bit long)</td>
 * </tr>
 * <tr>
 * <td>decimal</td>
 * <td>BigDecimal</td>
 * <td>Variable-precision decimal</td>
 * </tr>
 * <tr>
 * <td>double</td>
 * <td>Double</td>
 * <td>64-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>float</td>
 * <td>Float</td>
 * <td>32-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>int</td>
 * <td>Integer</td>
 * <td>32-bit signed int</td>
 * </tr>
 * <tr>
 * <td>text</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>timestamp</td>
 * <td>Date</td>
 * <td>A timestamp</td>
 * </tr>
 * <tr>
 * <td>uuid</td>
 * <td>UUID</td>
 * <td>Type 1 or type 4 UUID</td>
 * </tr>
 * <tr>
 * <td>varchar</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>varint</td>
 * <td>BigInteger</td>
 * <td>Arbitrary-precision integer</td>
 * </tr>
 * </table>
 * 
 */
class CassandraResultSet extends AbstractResultSet implements CassandraResultSetExtras
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraResultSet.class);

    public static final int DEFAULT_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    public static final int DEFAULT_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    public static final int DEFAULT_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;

    /**
     * The rows iterator.
     */

    int rowNumber = 0;
    // the current row key when iterating through results.
    private byte[] curRowKey = null;

    /**
     * The values.
     */
//    private List<TypedColumn> values = new ArrayList<TypedColumn>();

    /**
     * The index map.
     */
//    private Map<String, Integer> indexMap = new HashMap<String, Integer>();

    private final CResultSetMetaData meta;

    private final CassandraStatement statement;

    private int resultSetType;

    private int fetchDirection;

    private boolean wasNull;

    private ColumnDefinitions schema;

    private com.datastax.driver.core.ResultSet cResultSet;

    private Row curRow;

    private final AbstractJdbcType[] jdbcTypes;

    /**
     * no argument constructor.
     */
    CassandraResultSet()
    {
        statement = null;
        meta = new CResultSetMetaData();
        jdbcTypes = null;
        schema = null;
        cResultSet = null;
    }

    /**
     * Instantiates a new cassandra result set from a CqlResult.
     */
    CassandraResultSet(CassandraStatement statement, com.datastax.driver.core.ResultSet resultSet) throws SQLException
    {
        this.cResultSet = resultSet;
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.schema = resultSet.getColumnDefinitions();

        // Initialize to column values from the first row
        // re-Initialize meta-data to column values from the first row (if data exists)
        // NOTE: that the first call to next() will HARMLESSLY re-write these values for the columns
        // NOTE: the row cursor is not advanced and sits before the first row
        meta = new CResultSetMetaData();

        jdbcTypes = getJdbcTypes();
    }

    AbstractJdbcType[] getJdbcTypes() {
      AbstractJdbcType[] jdbcTypes = new AbstractJdbcType[schema.size()];

      for (int i = 0; i < schema.size(); i++) {
        try {
          jdbcTypes[i] =  HandleObjects.getType(schema.getType(i).asJavaClass());
        } catch (SQLException e) {
          jdbcTypes[i] = null;
        }
      }

      return jdbcTypes;
    }


    private boolean hasMoreRows()
    {
       if (cResultSet != null)
        return (!cResultSet.isExhausted());
      else return false;
    }

    public boolean absolute(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void afterLast() throws SQLException
    {
        if (resultSetType == TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void beforeFirst() throws SQLException
    {
        if (resultSetType == TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    private void checkColumnNo(int column) throws SQLException
    {
        // 1 <= column <= size()
        if (column < 1 || column > schema.size()) throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, String.valueOf(column)) + " " + schema.size());
    }

    private void checkName(String name) throws SQLException
    {
        if (!schema.contains(name)) throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
    }

    private void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLRecoverableException(WAS_CLOSED_RSLT);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    public void close() throws SQLException
    {
    }

    public int findColumn(String name) throws SQLException
    {
        checkNotClosed();
        checkName(name);
        return schema.getIndexOf(name) + 1;
    }

    public boolean first() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /** @deprecated */
    public BigDecimal getBigDecimal(int column, int scale) throws SQLException
    {
        checkColumnNo(column);
        return getBigDecimal(column).setScale(scale);
    }

    public BigDecimal getBigDecimal(String name) throws SQLException
    {
        checkName(name);
        return getBigDecimal(schema.getIndexOf(name) + 1);
    }

    /** @deprecated */
    public BigDecimal getBigDecimal(String name, int scale) throws SQLException
    {
        checkName(name);
        return getBigDecimal(name).setScale(scale);
    }

    public BigDecimal getBigDecimal(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);

        if (wasNull) return BigDecimal.ZERO;
        Class<?> type = schema.getType(index).asJavaClass();

        // TODO Fix this if (value instanceof BigDecimal) return (BigDecimal) value;

        if (type == Long.class) return BigDecimal.valueOf(curRow.getLong(index));

        if (type == Double.class) return BigDecimal.valueOf(curRow.getDouble(index));

        // TODO - fix this if (value instanceof BigInteger) return new BigDecimal((BigInteger) value);

        try
        {
            if (type == String.class) return (new BigDecimal(curRow.getString(index)));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "BigDecimal"));
    }

    public BigInteger getBigInteger(String name) throws SQLException
    {
      // TODO - fix this
        checkName(name);
      return getBigInteger(schema.getIndexOf(name) + 1);
    }

    public BigInteger getBigInteger(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);

        if (wasNull) return BigInteger.ZERO;
        Class<?> type = schema.getType(index).asJavaClass();

        // TODO - fix this if (value instanceof BigInteger) return curRow.getB;

        if (type == Integer.class) return BigInteger.valueOf(curRow.getInt(index));

        if (type == Long.class) return BigInteger.valueOf(curRow.getLong(index));

        try
        {
            if (type == String.class) return (new BigInteger(curRow.getString(index)));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "BigInteger"));
    }

    public boolean getBoolean(String name) throws SQLException
    {
        checkName(name);
        return getBoolean(schema.getIndexOf(name) + 1);
    }

    public boolean getBoolean(int column) throws SQLException
    {
        int index = column - 1;  // SQL is 1 based - java driver is 0-based
        checkNotClosed();
        wasNull = curRow.isNull(index);

        if (wasNull) return false;

        Class<?> type = schema.getType(index).asJavaClass();

        if (type == Boolean.class) return curRow.getBool(index);

        if (type == Integer.class) return curRow.getInt(index) == 0;

        if (type == Long.class) return curRow.getLong(index) == 0;

     // TODO - FIX THIS   if (type.asJavaClass() == BigInteger.class) return curRow.ge == 0 ? false : true);

        if (type == String.class)
        {
            String str = getString(column);   // note - we use "column here" as we call a public get
            if (str.equalsIgnoreCase("true")) return true;
            if (str.equalsIgnoreCase("false")) return false;

            throw new SQLSyntaxErrorException(String.format(NOT_BOOLEAN, str));
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "Boolean"));
    }


    public byte getByte(String name) throws SQLException
    {
        checkName(name);
        return getByte(schema.getIndexOf(name) + 1);
    }

    public final byte getByte(int column) throws SQLException
    {
      checkColumnNo(column);
      int index = column - 1;
      checkNotClosed();
      Class<?> type = schema.getType(index).asJavaClass();

        wasNull = curRow.isNull(index);

        if (wasNull) return 0;

        if (type == Integer.class) return (new Integer(curRow.getInt(index))).byteValue();

        if (type == Long.class) return (new Long(curRow.getInt(index))).byteValue();

       // TODO - fix this if (value instanceof BigInteger) return ((BigInteger) value).byteValue();

        try
        {
            if (type == String.class) return (new Byte(curRow.getString(index)));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "Byte"));
    }

    public byte[] getBytes(String name) throws SQLException
    {
      return getBytes(schema.getIndexOf(name) + 1);
    }

    public byte[] getBytes(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);
        if (wasNull) return null;
        return curRow.getBytes(index).array();
    }

// TODO - probably remove this
//    public TypedColumn getColumn(int index) throws SQLException
//    {
//        checkColumnNo(index);
//        checkNotClosed();
//        return values.get(index - 1);
//    }
//
//    public TypedColumn getColumn(String name) throws SQLException
//    {
//        checkName(name);
//        checkNotClosed();
//        return values.get(indexMap.get(name).intValue());
//    }

    public int getConcurrency() throws SQLException
    {
        checkNotClosed();
        return statement.getResultSetConcurrency();
    }

    public Date getDate(int column, Calendar calendar) throws SQLException
    {
        checkColumnNo(column);
        return getDate(column);
    }

    public Date getDate(String name) throws SQLException
    {
        checkName(name);
        return getDate(schema.getIndexOf(name) + 1);
    }

    public Date getDate(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
      return getDate(name);
    }

    public Date getDate(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);

        if (wasNull) return null;

        Class<?> type = schema.getType(index).asJavaClass();

        if (type == Long.class) return new Date(curRow.getLong(index));

        if (type == java.util.Date.class) return new Date(curRow.getDate(index).getTime());

        try
        {
            if (type == String.class) return Date.valueOf(curRow.getString(index));
        }
        catch (IllegalArgumentException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "SQL Date"));
    }

    public double getDouble(String name) throws SQLException
    {
        checkName(name);
        return getDouble(schema.getIndexOf(name) + 1);
    }

    public final double getDouble(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);
        Class<?> type = schema.getType(index).asJavaClass();

        if (wasNull) return 0.0;

        if (type == Double.class) return curRow.getDouble(index);

        if (type == Float.class) return curRow.getFloat(index);

        if (type == Integer.class) return (double) curRow.getInt(index);

        if (type ==  Long.class) return (double) curRow.getLong(index);

       // TODO - FIX  if (type ==  BigInteger) return new Double(((BigInteger) value).doubleValue());

        try
        {
            if (type == String.class) return new Double(curRow.getString(index));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "Double"));
    }

    public int getFetchDirection() throws SQLException
    {
        checkNotClosed();
        return fetchDirection;
    }

    public int getFetchSize() throws SQLException
    {
        checkNotClosed();
        return statement.getFetchSize();
    }

    public float getFloat(String name) throws SQLException
    {
        checkName(name);
      return getFloat(schema.getIndexOf(name) + 1);
    }

    public final float getFloat(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);
        if (wasNull) return (float) 0.0;

        Class<?> type = schema.getType(index).asJavaClass();

        if (type == Float.class) return curRow.getFloat(index);

        if (type == Double.class) return (float) curRow.getDouble(index);

        if (type == Integer.class) return (float) curRow.getInt(index);

        if (type == Long.class) return (float) curRow.getLong(index);

        // TODO - fix this if (value instanceof BigInteger) return new Float(((BigInteger) value).floatValue());

        try
        {
            if (type == String.class) return new Float(curRow.getString(index));
        }
        catch (NumberFormatException e)
        {
            throw new SQLException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "Float"));
    }

    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        return statement.getResultSetHoldability();
    }

    public int getInt(String name) throws SQLException
    {
        checkName(name);
      return getInt(schema.getIndexOf(name) + 1);
    }

    public int getInt(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);

        if (wasNull) return 0;
        Class<?> type = schema.getType(index).asJavaClass();

        if (type == Integer.class) return curRow.getInt(index);

        if (type == Long.class) return (int) curRow.getLong(index);

        // TODO - fix this if (value instanceof BigInteger) return ((BigInteger) value).intValue();

        try
        {
            if (type == String.class) return Integer.parseInt(curRow.getString(index));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "int"));
    }

    public byte[] getKey() throws SQLException
    {
        return curRowKey;
    }

    public List<?> getList(String name) throws SQLException
    {
      // TODO - FIX THIS
        checkName(name);
       return getList(schema.getIndexOf(name) + 1);
    }

    public List<?> getList(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);
        Class<?> type = schema.getType(index).asJavaClass();

        if (schema.getType(index).isCollection() /* TODO  - validate is LIST here */) throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE,
            type.getSimpleName(),
            "List"));
        return curRow.getList(index, Object.class);
    }

    public long getLong(String name) throws SQLException
    {
        checkName(name);
        return getLong(schema.getIndexOf(name) + 1);
    }

    public long getLong(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);
        Class<?> type = schema.getType(index).asJavaClass();

        if (wasNull) return 0L;

        if (type == Long.class) return curRow.getLong(index);

        if (type == Integer.class) return (long) curRow.getInt(index);

        //TODO - FIX if (value instanceof BigInteger) return getBigInteger(column).longValue();


        try
        {
            if (type == String.class) return (Long.parseLong(curRow.getString(index)));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "Long"));
    }

    public Map<?, ?> getMap(String name) throws SQLException
    {
        checkName(name);
        return getMap(schema.getIndexOf(name) + 1);
    }

    public Map<?, ?> getMap(int column) throws SQLException
    {
      // TODO - FIX THIS
      int index = column - 1;
//        checkNotClosed();
//        Object value = column.getValue();
//        wasNull = value == null;
//        if (column.getCollectionType() != CollectionType.MAP) throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE,
//            value.getClass().getSimpleName(),
//            "Map"));
//        return (Map<?, ?>) value;
      return null;
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        return meta;
    }

    public Object getObject(String name) throws SQLException
    {
        checkName(name);
        return getObject(schema.getIndexOf(name) + 1);
    }


    public Object getObject(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
      wasNull = curRow.isNull(index);
      if (wasNull) return null;

      Class<?> type = schema.getType(index).asJavaClass();

      // handle date properly for expectations of a JDBC caller
      if (type == Integer.class) return curRow.getInt(index);
      if (type == Long.class) return curRow.getLong(index);
      if (type == String.class) return curRow.getString(index);
      if (type == Boolean.class) return curRow.getBool(index);
      if (type == Float.class) return curRow.getFloat(index);
      if (type == Double.class) return curRow.getDouble(index);
      if (type == ByteBuffer.class) return curRow.getBytes(index).array();

      // TODO - there are more types

      if (type == java.util.Date.class) {
          return new Timestamp(curRow.getDate(index).getTime());
        }

      // TODO - Throw something
      return null;

    }

    public int getRow() throws SQLException
    {
        checkNotClosed();
        return rowNumber;
    }

    public RowId getRowId(String name) throws SQLException
    {
        checkName(name);
        return getRowId(schema.getIndexOf(name) + 1);
    }

    public final RowId getRowId(int column) throws SQLException
    {
      checkNotClosed();
      int index = column - 1;
      wasNull = curRow.isNull(index);
      if (wasNull) return null;

      Object rowValue = getObject(index + 1);
        return new CassandraRowId(null);  // TODO fix this !!!!  need to gen a rowid for different types
    }

    public Set<?> getSet(String name) throws SQLException
    {
        checkName(name);
        return getSet(schema.getIndexOf(name) + 1);
    }

    public Set<?> getSet(int column) throws SQLException
    {
      int index = column - 1;
      // TODO - FIX THIS!!!
//        checkNotClosed();
//        Object value = column.getValue();
//        wasNull = value == null;
//        if (column.getCollectionType() != CollectionType.SET) throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE,
//            value.getClass().getSimpleName(),
//            "Set"));
//        return (Set<?>) value;
      return null;
    }

    public short getShort(String name) throws SQLException
    {
        checkName(name);
        return getShort(schema.getIndexOf(name) + 1);
    }

    public final short getShort(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);

        if (wasNull) return 0;
        Class<?> type = schema.getType(index).asJavaClass();

        if (type == Integer.class) return ((Integer) curRow.getInt(index)).shortValue();

        if (type == Long.class) return ((Long) curRow.getLong(index)).shortValue();


        // TODO - FIX THIS if (value instanceof BigInteger) return ((BigInteger) value).shortValue();

        try
        {
            if (type ==  String.class) return (new Short(curRow.getString(index)));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "Short"));
    }

    public Statement getStatement() throws SQLException
    {
        checkNotClosed();
        return statement;
    }

    public String getString(String name) throws SQLException
    {
        checkName(name);
        return getString(schema.getIndexOf(name) + 1);
    }

    public String getString(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);
        if (wasNull) return null;

        Object value = getObject(column);  // we're using a public function - so use column here
        return value.toString();
    }

    public Time getTime(int column, Calendar calendar) throws SQLException
    {
        checkColumnNo(column);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(column);
    }

    public Time getTime(String name) throws SQLException
    {
        checkName(name);
        return getTime(schema.getIndexOf(name) + 1);
    }

    public Time getTime(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(name);
    }

    public Time getTime(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);

        if (wasNull) return null;
        Class<?> type = schema.getType(index).asJavaClass();

        if (type == Long.class) return new Time(curRow.getLong(index));

        if (type == java.util.Date.class) return new Time(curRow.getDate(index).getTime());

        try
        {
            if (type == String.class) return Time.valueOf(curRow.getString(index));
        }
        catch (IllegalArgumentException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "SQL Time"));
    }

    public Timestamp getTimestamp(int column, Calendar calendar) throws SQLException
    {
        checkColumnNo(column);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(column);
    }

    public Timestamp getTimestamp(String name) throws SQLException
    {
        checkName(name);
        return getTimestamp(schema.getIndexOf(name) + 1);
    }

    public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(name);
    }

    public Timestamp getTimestamp(int column) throws SQLException
    {
        int index = column - 1;
        checkNotClosed();
        wasNull = curRow.isNull(index);

        if (wasNull) return null;
        Class<?> type = schema.getType(index).asJavaClass();

        if (type == Long.class) return new Timestamp(curRow.getLong(index));

        if (type == java.util.Date.class) return new Timestamp(curRow.getDate(index).getTime());

        try
        {
            if (type == String.class) return Timestamp.valueOf(curRow.getString(index));
        }
        catch (IllegalArgumentException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, type.getSimpleName(), "SQL Timestamp"));
    }

    public int getType() throws SQLException
    {
        checkNotClosed();
        return resultSetType;
    }

    // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in URL format?
    public URL getURL(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public URL getURL(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    // These Methods are planned to be implemented soon; but not right now...
    // Each set of methods has a more detailed set of issues that should be considered fully...


    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }


    public boolean isAfterLast() throws SQLException
    {
        checkNotClosed();
        return rowNumber == Integer.MAX_VALUE;
    }

    public boolean isBeforeFirst() throws SQLException
    {
        checkNotClosed();
        return rowNumber == 0;
    }

    public boolean isClosed() throws SQLException
    {
      // TODO - FIX THIS - not sure when to consider it closed.
        return false;
    }

    public boolean isFirst() throws SQLException
    {
        checkNotClosed();
        return rowNumber == 1;
    }

    public boolean isLast() throws SQLException
    {
        checkNotClosed();
        return !cResultSet.isExhausted();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return CassandraResultSetExtras.class.isAssignableFrom(iface);
    }

    // Navigation between rows within the returned set of rows
    // Need to use a list iterator so next() needs completely re-thought

    public boolean last() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public synchronized boolean next() throws SQLException
    {
      if (hasMoreRows())
      {

        curRow = cResultSet.one();
        rowNumber++;
        return true;
      }
      else
      {
        rowNumber = Integer.MAX_VALUE;
        return false;
      }
    }

    private String bbToString(ByteBuffer buffer)
    {
        try
        {
            return string(buffer);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }

    }

// TODO - maybe remove this
//    private TypedColumn createColumn(Column column)
//    {
//        assert column != null;
//        assert column.name != null;
//
//        AbstractJdbcType<?> keyType = null;
//        CollectionType type = CollectionType.NOT_COLLECTION;
//        String nameType = schema.name_types.get(column.name);
//        if (nameType == null) nameType = "AsciiType";
//        AbstractJdbcType<?> comparator = TypesMap.getTypeForComparator(nameType == null ? schema.default_name_type : nameType);
//        String valueType = schema.value_types.get(column.name);
//        AbstractJdbcType<?> validator = TypesMap.getTypeForComparator(valueType == null ? schema.default_value_type : valueType);
//        if (validator == null)
//        {
//            int index = valueType.indexOf("(");
//            assert index > 0;
//
//            String collectionClass = valueType.substring(0, index);
//            if (collectionClass.endsWith("ListType")) type = CollectionType.LIST;
//            else if (collectionClass.endsWith("SetType")) type = CollectionType.SET;
//            else if (collectionClass.endsWith("MapType")) type = CollectionType.MAP;
//
//            String[] split = valueType.substring(index + 1, valueType.length() - 1).split(",");
//            if (split.length > 1)
//            {
//                keyType = TypesMap.getTypeForComparator(split[0]);
//                validator = TypesMap.getTypeForComparator(split[1]);
//            }
//            else validator = TypesMap.getTypeForComparator(split[0]);
//
//        }
//
//        TypedColumn tc = new TypedColumn(column, comparator, validator, keyType, type);
//
//        if (logger.isTraceEnabled()) logger.trace("tc = " + tc);
//
//        return tc;
//    }

    public boolean previous() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean relative(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        checkNotClosed();

        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN)
        {
            if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD)) throw new SQLSyntaxErrorException("attempt to set an illegal direction : " + direction);
            fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }

    public void setFetchSize(int size) throws SQLException
    {
        checkNotClosed();
        if (size < 0) throw new SQLException(String.format(BAD_FETCH_SIZE, size));
        statement.setFetchSize(size);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface.equals(CassandraResultSetExtras.class)) return (T) this;

        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }

    /**
     * RSMD implementation. The metadata returned refers to the column
     * values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData
    {
        /**
         * return the Cassandra Cluster Name as the Catalog
         */
        public String getCatalogName(int column) throws SQLException
        {
            checkColumnNo(column);
            return statement.connection.getCatalog();
        }

        public String getColumnClassName(int column) throws SQLException
        {
            checkColumnNo(column);
            return schema.getType(column - 1).asJavaClass().getName();
        }

        public int getColumnCount() throws SQLException
        {
            if (schema != null)
              return schema.size();
            else return 0;
        }

        public int getColumnDisplaySize(int column) throws SQLException
        {
            checkColumnNo(column);
            String stringValue = getString(column);
            return (stringValue == null ? -1 : stringValue.length());
        }

        public String getColumnLabel(int column) throws SQLException
        {
            checkColumnNo(column);
            return getColumnName(column);
        }

        public String getColumnName(int column) throws SQLException
        {
            checkColumnNo(column);
            return schema.getName(column - 1);
        }

        public int getColumnType(int column) throws SQLException
        {
            checkColumnNo(column);
            return jdbcTypes[column -1].getJdbcType();
        }

        /**
         * Spec says "database specific type name"; for Cassandra this means the AbstractType.
         */
        public String getColumnTypeName(int column) throws SQLException
        {
            checkColumnNo(column);
            return schema.getType(column - 1).asJavaClass().getSimpleName();
        }

        public int getPrecision(int column) throws SQLException
        {
          // TODO - FIX THIS
//            checkColumnNo(column);
//            return col.getValueType().getPrecision(col.getValue());
            return 0;
        }

        public int getScale(int column) throws SQLException
        {
          // Todo - FIX THIS
            checkColumnNo(column);
           // return tc.getValueType().getScale(tc.getValue());
            return 0;
        }

        /**
         * return the DEFAULT current Keyspace as the Schema Name
         */
        public String getSchemaName(int column) throws SQLException
        {
            checkColumnNo(column);
            return statement.connection.getSchema();
        }

        public String getTableName(int column) throws SQLException
        {
            throw new SQLFeatureNotSupportedException();
        }

        public boolean isAutoIncrement(int column) throws SQLException
        {
            // TODO - double check this
            checkColumnNo(column);
            return false; // todo: check Value is correct.
        }

        public boolean isCaseSensitive(int column) throws SQLException
        {
            // TODO Double check this
            checkColumnNo(column);
            return true;
        }

        public boolean isCurrency(int index) throws SQLException
        {
            checkColumnNo(index);
            return jdbcTypes[index - 1].isCurrency();
        }

        public boolean isDefinitelyWritable(int column) throws SQLException
        {
            checkColumnNo(column);
            return isWritable(column);
        }

        /**
         * absence is the equivalent of null in Cassandra
         */
        public int isNullable(int column) throws SQLException
        {
            checkColumnNo(column);
            return ResultSetMetaData.columnNullable;
        }

        public boolean isReadOnly(int column) throws SQLException
        {
            checkColumnNo(column);
            return column == 0;
        }

        public boolean isSearchable(int column) throws SQLException
        {
            checkColumnNo(column);
            return false;
        }

        public boolean isSigned(int column) throws SQLException
        {
            checkColumnNo(column);
            return jdbcTypes[column - 1].isSigned();
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException
        {
            return false;
        }

        public boolean isWritable(int column) throws SQLException
        {
            checkColumnNo(column);
            return column > 0;
        }

        public <T> T unwrap(Class<T> iface) throws SQLException
        {
            throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }
    }
}
