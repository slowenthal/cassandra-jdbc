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

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import java.nio.ByteBuffer;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import com.datastax.driver.core.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public  class MetadataResultSets
{
    static final String TABLE_CONSTANT = "TABLE";

    public static final MetadataResultSets instance = new MetadataResultSets();
    
    
    // Private Constructor
    private MetadataResultSets() {}


  // Convenience method for cleaner code.  Why don't they have macros in Java???
  private static CannedRow mkRow(Object... objects) {
    return new CannedRow(objects);
  }


    public  CassandraResultSet makeTableTypes(CassandraStatement statement) throws SQLException
    {

      CannedResultSet cr = new CannedResultSet("TABLE_TYPE", DataType.text())
              .addRow(TABLE_CONSTANT);

        return new CassandraResultSet(statement,cr);
    }

  public CassandraResultSet makeImportedKeys(CassandraStatement statement) throws SQLException {

//  PKTABLE_CAT String => primary key table catalog being imported (may be null)
//  PKTABLE_SCHEM String => primary key table schema being imported (may be null)
//  PKTABLE_NAME String => primary key table name being imported
//  PKCOLUMN_NAME String => primary key column name being imported
//  FKTABLE_CAT String => foreign key table catalog (may be null)
//  FKTABLE_SCHEM String => foreign key table schema (may be null)
//  FKTABLE_NAME String => foreign key table name
//  FKCOLUMN_NAME String => foreign key column name
//  KEY_SEQ short => sequence number within a foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
//  UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
//  importedNoAction - do not allow update of primary key if it has been imported
//  importedKeyCascade - change imported key to agree with primary key update
//  importedKeySetNull - change imported key to NULL if its primary key has been updated
//  importedKeySetDefault - change imported key to default values if its primary key has been updated
//  importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
//  DELETE_RULE short => What happens to the foreign key when primary is deleted.
//  importedKeyNoAction - do not allow delete of primary key if it has been imported
//  importedKeyCascade - delete rows that import a deleted key
//  importedKeySetNull - change imported key to NULL if its primary key has been deleted
//  importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
//  importedKeySetDefault - change imported key to default if its primary key has been deleted
//  FK_NAME String => foreign key name (may be null)
//  PK_NAME String => primary key name (may be null)
//  DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit

    CannedResultSet cr = new CannedResultSet(
            "PKTABLE_CAT", DataType.text(),
            "PKTABLE_SCHEM", DataType.text(),
            "PKTABLE_NAME", DataType.text(),
            "PKCOLUMN_NAME", DataType.text(),
            "FKTABLE_CAT", DataType.text(),
            "FKTABLE_SCHEM", DataType.text(),
            "FKTABLE_NAME", DataType.text(),
            "FKCOLUMN_NAME", DataType.text(),
            "KEY_SEQ", DataType.cint(),
            "UPDATE_RULE", DataType.cint(),
            "DELETE_RULE", DataType.cint(),
            "FK_NAME", DataType.text(),
            "PK_NAME", DataType.text(),
            "DEFERRABILITY", DataType.cint());

    return new CassandraResultSet(statement, cr);
  }

  public CassandraResultSet makeTypeInfo(CassandraStatement statement) throws SQLException {
    // This will be empty
//
//    TYPE_NAME String => Type name
//    DATA_TYPE int => SQL data type from java.sql.Types
//    PRECISION int => maximum precision
//    LITERAL_PREFIX String => prefix used to quote a literal (may be null)
//    LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
//    CREATE_PARAMS String => parameters used in creating the type (may be null)
//    NULLABLE short => can you use NULL for this type.
//            typeNoNulls - does not allow NULL values
//    typeNullable - allows NULL values
//    typeNullableUnknown - nullability unknown
//    CASE_SENSITIVE boolean=> is it case sensitive.
//            SEARCHABLE short => can you use "WHERE" based on this type:
//    typePredNone - No support
//    typePredChar - Only supported with WHERE .. LIKE
//    typePredBasic - Supported except for WHERE .. LIKE
//    typeSearchable - Supported for all WHERE ..
//    UNSIGNED_ATTRIBUTE boolean => is it unsigned.
//            FIXED_PREC_SCALE boolean => can it be a money value.
//    AUTO_INCREMENT boolean => can it be used for an auto-increment value.
//            LOCAL_TYPE_NAME String => localized version of type name (may be null)
//    MINIMUM_SCALE short => minimum scale supported
//    MAXIMUM_SCALE short => maximum scale supported
//    SQL_DATA_TYPE int => unused
//    SQL_DATETIME_SUB int => unused
//    NUM_PREC_RADIX int => usually 2 or 10

    CannedResultSet cr = new CannedResultSet(
    "TYPE_NAME", DataType.text(),
    "DATA_TYPE", DataType.cint(),
    "PRECISION", DataType.cint(),
    "LITERAL_PREFIX", DataType.text(),
    "LITERAL_SUFFIX", DataType.text(),
    "CREATE_PARAMS", DataType.text(),
    "NULLABLE", DataType.cint(),
    "CASE_SENSITIVE", DataType.cboolean(),
    "UNSIGNED_ATTRIBUTE", DataType.cboolean(),
    "FIXED_PREC_SCALE", DataType.cboolean(),
    "AUTO_INCREMENT", DataType.cboolean(),
    "LOCAL_TYPE_NAME", DataType.text(),
    "MINIMUM_SCALE", DataType.cint(),
    "MAXIMUM_SCALE", DataType.cint(),
    "SQL_DATA_TYPE", DataType.cint(),
    "SQL_DATETIME_SUB", DataType.cint(),
    "NUM_PREC_RADIX", DataType.cint()
    );

    return new CassandraResultSet(statement, cr);
  }

  public  CassandraResultSet makeCatalogs(CassandraStatement statement) throws SQLException
  {
    CannedResultSet cr = new CannedResultSet("TABLE_CAT", DataType.text())
            .addRow(statement.connection.getCatalog());

    return new CassandraResultSet(statement,cr);
  }

    public  CassandraResultSet makeSchemas(CassandraStatement statement, String schemaPattern) throws SQLException
    {
        if ("%".equals(schemaPattern)) schemaPattern = null;

        // TABLE_SCHEM String => schema name
        // TABLE_CATALOG String => catalog name (may be null)

        String query = "SELECT keyspace_name FROM system.schema_keyspaces";
        if (schemaPattern!=null) query = query + " where keyspace_name = '" + schemaPattern + "'";

        String catalog = statement.connection.getCatalog();

        // use schemas with the key in column number 2 (one based)
        CannedResultSet cr = new CannedResultSet(
                                "TABLE_SCHEM", DataType.text(),
                                "TABLE_CATALOG",DataType.text()) ;

      CassandraResultSet result = (CassandraResultSet)statement.executeQuery(query);

        while (result.next())
        {
              cr.addRow(result.getString(1), catalog);
        }

        // just return the empty result if there were no rows
    //    if (cr.isEmpty()) return result;

      cr.sortAlpha(0);

      result = new CassandraResultSet(statement,cr);
        return result;
    }
    
    public CassandraResultSet makeTables(CassandraStatement statement, String schemaPattern, String tableNamePattern) throws SQLException
    {
        //   1.   TABLE_CAT String => table catalog (may be null)
        //   2.   TABLE_SCHEM String => table schema (may be null)
        //   3.   TABLE_NAME String => table name
        //   4.   TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
        //   5.   REMARKS String => explanatory comment on the table
        //   6.   TYPE_CAT String => the types catalog (may be null)
        //   7.   TYPE_SCHEM String => the types schema (may be null)
        //   8.   TYPE_NAME String => type name (may be null)
        //   9.   SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
        //   10.  REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)        

        if ("%".equals(schemaPattern)) schemaPattern = null;
        if ("%".equals(tableNamePattern)) tableNamePattern = null;
        
        // example query to retrieve tables
        // SELECT keyspace_name,columnfamily_name,comment from schema_columnfamilies WHERE columnfamily_name = 'Test2';
        // Note - the CQL3 will return the results in
        StringBuilder query = new StringBuilder("SELECT keyspace_name,columnfamily_name,comment FROM system.schema_columnfamilies");

        int filterCount = 0;
        if (schemaPattern != null) filterCount++;
        if (tableNamePattern != null) filterCount++;

        // check to see if it is qualified
        if (filterCount > 0)
        {
            String expr = "%s = '%s'";
            query.append(" WHERE ");
            if (schemaPattern != null) 
            {
            	query.append(String.format(expr, "keyspace_name", schemaPattern));
                filterCount--;
                if (filterCount > 0) query.append(" AND ");
            }
            if (tableNamePattern != null) query.append(String.format(expr, "columnfamily_name", tableNamePattern));
            query.append(" ALLOW FILTERING;");
        }
        // System.out.println(query.toString());

        String catalog = statement.connection.getCatalog();

      // determine the schemas
      CassandraResultSet result = (CassandraResultSet)statement.executeQuery(query.toString());

      CannedResultSet cr = new CannedResultSet(
                      "TABLE_CAT",                 DataType.text(),
                      "TABLE_SCHEM",               DataType.text(),
                      "TABLE_NAME",                DataType.text(),
                      "TABLE_TYPE",                DataType.text(),
                      "REMARKS",                   DataType.text(),
                      "TYPE_CAT",                  DataType.text(),
                      "TYPE_SCHEM",                DataType.text(),
                      "TYPE_NAME",                 DataType.text(),
                      "SELF_REFERENCING_COL_NAME", DataType.text(),
                      "REF_GENERATION",            DataType.text());

        while (result.next())
        {
           cr.addRow(catalog,               // TABLE_CAT
                   result.getString(1),     // TABLE_SCHEM
                   result.getString(2),     // TABLE_NAME
                   TABLE_CONSTANT,          // TABLE_TYPE
                   result.getString(3),     // REMARKS
                   null,                    // TYPE_CAT
                   null,                    // TYPE_SCHEM
                   null,                    // TYPE_NAME
                   null,                    // SELF_REFERENCING_COL_NAME
                   null                     // REF_GENERATION
           );
        }

      cr.sortAlpha(1,2);
      return new CassandraResultSet(statement, cr);
    }
        
    public CassandraResultSet makeColumns(CassandraStatement statement, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException
    {
        // 1.TABLE_CAT String => table catalog (may be null)
        // 2.TABLE_SCHEM String => table schema (may be null)
        // 3.TABLE_NAME String => table name
        // 4.COLUMN_NAME String => column name
        // 5.DATA_TYPE int => SQL type from java.sql.Types
        // 6.TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
        // 7.COLUMN_SIZE int => column size.
        // 8.BUFFER_LENGTH is not used.
        // 9.DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
        // 10.NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        // 11.NULLABLE int => is NULL allowed. - columnNoNulls - might not allow NULL values
        // - columnNullable - definitely allows NULL values
        // - columnNullableUnknown - nullability unknown
        //
        // 12.REMARKS String => comment describing column (may be null)
        // 13.COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in
        // single quotes (may be null)
        // 14.SQL_DATA_TYPE int => unused
        // 15.SQL_DATETIME_SUB int => unused
        // 16.CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        // 17.ORDINAL_POSITION int => index of column in table (starting at 1)
        // 18.IS_NULLABLE String => ISO rules are used to determine the nullability for a column. - YES --- if the parameter can include
        // NULLs
        // - NO --- if the parameter cannot include NULLs
        // - empty string --- if the nullability for the parameter is unknown
        //
        // 19.SCOPE_CATLOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
        // 20.SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
        // 21.SCOPE_TABLE String => table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF)
        // 22.SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if
        // DATA_TYPE isn't DISTINCT or user-generated REF)
        // 23.IS_AUTOINCREMENT String => Indicates whether this column is auto incremented - YES --- if the column is auto incremented
        // - NO --- if the column is not auto incremented
        // - empty string --- if it cannot be determined whether the column is auto incremented parameter is unknown
        // 24. IS_GENERATEDCOLUMN String => Indicates whether this is a generated column ï YES --- if this a generated column
        // - NO --- if this not a generated column
        // - empty string --- if it cannot be determined whether this is a generated column

        if ("%".equals(schemaPattern)) schemaPattern = null;
        if ("%".equals(tableNamePattern)) tableNamePattern = null;
        if ("%".equals(columnNamePattern)) columnNamePattern = null;

        StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, column_name, validator FROM system.schema_columns");

        int filterCount = 0;
        if (schemaPattern != null) filterCount++;
        if (tableNamePattern != null) filterCount++;
        if (columnNamePattern != null) filterCount++;

        // check to see if it is qualified
        if (filterCount > 0)
        {
            String expr = "%s = '%s'";
            query.append(" WHERE ");
            if (schemaPattern != null) 
            {
            	query.append(String.format(expr, "keyspace_name", schemaPattern));
                filterCount--;
                if (filterCount > 0) query.append(" AND ");
            }
            if (tableNamePattern != null) 
            {
            	query.append(String.format(expr, "columnfamily_name", tableNamePattern));
                filterCount--;
                if (filterCount > 0) query.append(" AND ");
            }
            if (columnNamePattern != null) query.append(String.format(expr, "column_name", columnNamePattern));
            query.append(" ALLOW FILTERING");
        }
        // System.out.println(query.toString());

        String catalog = statement.connection.getCatalog();

      // determine the schemas
      CassandraResultSet result = (CassandraResultSet)statement.executeQuery(query.toString());

      CannedResultSet cr = new CannedResultSet(
                      "TABLE_CAT",            DataType.text(),
                      "TABLE_SCHEM",          DataType.text(),
                      "TABLE_NAME",           DataType.text(),
                      "COLUMN_NAME",          DataType.text(),
                      "DATA_TYPE",            DataType.cint(),
                      "TYPE_NAME ",           DataType.text(),
                      "COLUMN_SIZE",          DataType.cint(),
                      "BUFFER_LENGTH",        DataType.cint(),
                      "DECIMAL_DIGITS",       DataType.cint(),
                      "NUM_PREC_RADIX",       DataType.cint(),
                      "NULLABLE",             DataType.cint(),
                      "REMARKS",              DataType.text(),
                      "COLUMN_DEF",           DataType.text(),
                      "SQL_DATA_TYPE",        DataType.cint(),
                      "SQL_DATETIME_SUB",     DataType.cint(),
                      "CHAR_OCTET_LENGTH",    DataType.cint(),
                      "ORDINAL_POSITION",     DataType.cint(),
                      "IS_NULLABLE",          DataType.text(),
                      "SCOPE_CATLOG",         DataType.text(),
                      "SCOPE_SCHEMA",         DataType.text(),
                      "SCOPE_TABLE",          DataType.text(),
                      "SOURCE_DATA_TYPE",     DataType.cint(),
                      "IS_AUTOINCREMENT",     DataType.text(),
                      "IS_GENERATEDCOLUMN",   DataType.text());

      int ordinalPosition = 0;

      while (result.next())
      {
        ordinalPosition ++;
        String validator = result.getString(4);
        AbstractJdbcType jtype = TypesMap.getTypeForComparator(validator);
        int dataType =  jtype == null ? Types.OTHER : jtype.getJdbcType();
        String typeName = validator.substring(validator.lastIndexOf('.') + 1);
        int npr =  (jtype != null && (jtype.getJdbcType() == Types.DECIMAL || jtype.getJdbcType() == Types.NUMERIC)) ? 10 : 2;
        Integer charOctetLength = (jtype instanceof JdbcAscii || jtype instanceof JdbcUTF8) ? Integer.MAX_VALUE : null;

        cr.addRow(catalog,               //  1. TABLE_CAT
                result.getString(1),     //  2. TABLE_SCHEM
                result.getString(2),     //  3. TABLE_NAME
                result.getString(3),     //  4. COLUMN_NAME
                dataType,                //  5. DATA_TYPE int
                typeName,                //  6. TYPE_NAME
                -1,                      //  7. COLUMN_SIZE int
                null,                    //  8. BUFFER_LENGTH int (not used)
                null,                    //  9. DECIMAL_DIGITS int (n/a)
                npr,                     // 10. NUM_PREC_RADIX (10 or 12)
                DatabaseMetaData.columnNullable,  // 11. NULLABLE int
                null,                    // 12. REMARKS
                null,                    // 13. COLUMN_DEF
                null,                    // 14. SQL_DATA_TYPE int => unused
                null,                    // 15. SQL_DATETIME_SUB int => unused
                charOctetLength,         // 16. CHAR_OCTET_LENGTH int
                ordinalPosition,         // 17. ORDINAL_POSITION int => index of column in table (starting at 1)
                "YES",                   // 18. IS_NULLABLE String
                null,                    // 19. SCOPE_CATLOG
                null,                    // 20. SCOPE_SCHEMA
                null,                    // 21. SCOPE_TABLE
                null,                    // 22. SOURCE_DATA_TYPE short
                "NO",                    // 23. IS_AUTOINCREMENT String
                "NO"                     // 24. IS_GENERATEDCOLUMN String
        );
      }


      return new CassandraResultSet(statement, cr);
    }
    
    public CassandraResultSet makeIndexes(CassandraStatement statement, String schema, String table, boolean unique, boolean approximate) throws SQLException
	{
		//1.TABLE_CAT String => table catalog (may be null) 
		//2.TABLE_SCHEM String => table schema (may be null) 
		//3.TABLE_NAME String => table name 
		//4.NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic 
		//5.INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic 
		//6.INDEX_NAME String => index name; null when TYPE is tableIndexStatistic 
		//7.TYPE short => index type: - tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions 
		//- tableIndexClustered - this is a clustered index 
		//- tableIndexHashed - this is a hashed index 
		//- tableIndexOther - this is some other style of index 
		//
		//8.ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic 
		//9.COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic 
		//10.ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic 
		//11.CARDINALITY int => When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index. 
		//12.PAGES int => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index. 
		//13.FILTER_CONDITION String => Filter condition, if any. (may be null) 
	
	    StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, column_name, component_index, index_name, index_options, index_type FROM system.schema_columns");
	
	    int filterCount = 0;
	    if (schema != null) filterCount++;
	    if (table != null) filterCount++;
	
	    // check to see if it is qualified
	    if (filterCount > 0)
	    {
	        String expr = "%s = '%s'";
	        query.append(" WHERE ");
	        if (schema != null) 
	        {
	        	query.append(String.format(expr, "keyspace_name", schema));
                filterCount--;
		        if (filterCount > 0) query.append(" AND ");
	        }
	        if (table != null) query.append(String.format(expr, "columnfamily_name", table));
	        query.append(" ALLOW FILTERING");
	    }
	    // System.out.println(query.toString());
	
	    String catalog = statement.connection.getCatalog();
	    Entry entryCatalog = new Entry("TABLE_CAT", bytes(catalog), Entry.ASCII_TYPE);
	
	    CassandraResultSet result;
	    List<Entry> col;
	    List<List<Entry>> rows = new ArrayList<List<Entry>>();
	
        int ordinalPosition = 0;
	    // define the columns
	    result = (CassandraResultSet) statement.executeQuery(query.toString());
	    while (result.next())
	    {
	        if (result.getString(7) == null) continue; //if there is no index_type its not an index
	        ordinalPosition++;

	        Entry entrySchema = new Entry("TABLE_SCHEM", bytes(result.getString(1)), Entry.ASCII_TYPE);
	        Entry entryTableName = new Entry("TABLE_NAME",
	            (result.getString(2) == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : bytes(result.getString(2)),
	            Entry.ASCII_TYPE);
	        Entry entryNonUnique = new Entry("NON_UNIQUE", bytes("true"),Entry.BOOLEAN_TYPE);
	        Entry entryIndexQualifier = new Entry("INDEX_QUALIFIER", bytes(catalog), Entry.ASCII_TYPE);
	        Entry entryIndexName = new Entry("INDEX_NAME", (result.getString(5) == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : bytes(result.getString(5))), Entry.ASCII_TYPE);
	        Entry entryType = new Entry("TYPE", bytes(DatabaseMetaData.tableIndexHashed), Entry.INT32_TYPE);
            Entry entryOrdinalPosition = new Entry("ORDINAL_POSITION", bytes(ordinalPosition), Entry.INT32_TYPE);
            Entry entryColumnName = new Entry("COLUMN_NAME",
                    (result.getString(3) == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : bytes(result.getString(3)),
                    Entry.ASCII_TYPE);
            Entry entryAoD = new Entry("ASC_OR_DESC",ByteBufferUtil.EMPTY_BYTE_BUFFER, Entry.ASCII_TYPE);
            Entry entryCardinality = new Entry("CARDINALITY", bytes(-1), Entry.INT32_TYPE);
            Entry entryPages = new Entry("PAGES", bytes(-1), Entry.INT32_TYPE);
            Entry entryFilter = new Entry("FILTER_CONDITION",ByteBufferUtil.EMPTY_BYTE_BUFFER, Entry.ASCII_TYPE);
	
	        col = new ArrayList<Entry>();
	        col.add(entryCatalog);
	        col.add(entrySchema);
	        col.add(entryTableName);
	        col.add(entryNonUnique);
	        col.add(entryIndexQualifier);
	        col.add(entryIndexName);
	        col.add(entryType);
	        col.add(entryOrdinalPosition);
	        col.add(entryColumnName);
	        col.add(entryAoD);
	        col.add(entryCardinality);
	        col.add(entryPages);
	        col.add(entryFilter);
	        rows.add(col);
	    }
	
	    // just return the empty result if there were no rows
	    if (rows.isEmpty()) return result;
	
//	    // use schemas with the key in column number 2 (one based)
//	    CqlResult cqlresult;
//	    try
//	    {
//	        cqlresult = makeCqlResult(rows, 1);
//	    }
//	    catch (CharacterCodingException e)
//	    {
//	        throw new SQLTransientException(e);
//	    }
//
//	    result = new CassandraResultSet(statement, cqlresult);
	    return result;

	}

	public List<PKInfo> getPrimaryKeys(CassandraStatement statement, String schema, String table) throws SQLException
	{
		StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, key_aliases, key_validator, column_aliases, comparator FROM system.schema_columnfamilies");
		
	    int filterCount = 0;
	    if (schema != null) filterCount++;
	    if (table != null) filterCount++;
	
	    // check to see if it is qualified
	    if (filterCount > 0)
	    {
	        String expr = "%s = '%s'";
	        query.append(" WHERE ");
	        if (schema != null) 
	        {
	        	query.append(String.format(expr, "keyspace_name", schema));
                filterCount--;
		        if (filterCount > 0) query.append(" AND ");
	        }
	        if (table != null) query.append(String.format(expr, "columnfamily_name", table));
	        query.append(" ALLOW FILTERING");
	    }
	    // System.out.println(query.toString());
	
	    List<PKInfo> retval = new ArrayList<PKInfo>();
	    CassandraResultSet result = (CassandraResultSet) statement.executeQuery(query.toString());
	    if (result.next()) // all is reported back in one json row
	    {
	    	String rschema = result.getString(1);
   	    	String rtable = result.getString(2);
	        String validator = result.getString(4);
	        String key_aliases = result.getString(3);
	        buildPKInfo(retval,rschema,rtable,key_aliases,validator);
	        
	        String comparator = result.getString(6);
	        String column_aliases = result.getString(5);
	        buildPKInfo(retval,rschema,rtable,column_aliases,comparator);
	    }
	    return retval;
	}
	
	private void buildPKInfo(List<PKInfo> retval,String schema, String table,String aliases,String validator)
	{
		String[] typeNames = new String[0];
    	int[] types = new int[0]; 
        if (validator != null)
        {
        	String[] validatorArray = new String[]{validator};
        	String check = "CompositeType";
        	int idx = validator.indexOf(check);
        	if (idx > 0)
        	{
        		validator = validator.substring(idx+check.length()+1,validator.length()-1);
        		validatorArray = validator.split(",");
        	}
        	types = new int[validatorArray.length];
        	typeNames = new String[validatorArray.length];
        	for (int i = 0; i < validatorArray.length; i++) 
        	{
	        	AbstractJdbcType jtype = TypesMap.getTypeForComparator(validatorArray[i]);
	        	types[i] = (jtype != null ? jtype.getJdbcType() : Types.OTHER);

	            int dotidx = validatorArray[i].lastIndexOf('.');
	            typeNames[i] = validatorArray[i].substring(dotidx + 1);
        	}
        }

        if (aliases != null)
        {
        	aliases = aliases.replace("[","");
        	aliases = aliases.replace("]","");
        	aliases = aliases.replace("\"","");
        	if (aliases.trim().length() != 0)
        	{
	        	String[] kaArray = aliases.split(",");
	        	for (int i = 0; i < kaArray.length; i++) 
	        	{
	        		PKInfo pki = new PKInfo();
	        		pki.name = kaArray[i];
	        		pki.schema = schema;
	        		pki.table = table;
	        		pki.type = (i < types.length ? types[i] : Types.OTHER);
	        		pki.typeName = (i < typeNames.length ? typeNames[i] : "unknown");
	        		retval.add(pki);
				}
        	}
        }
	}
	
	private class PKInfo
	{
		public String typeName;
		public String schema;
		public String table;
		public String name;
		public int type;
	}
	
	public CassandraResultSet makePrimaryKeys(CassandraStatement statement, String schema, String table) throws SQLException
	{
		//1.TABLE_CAT String => table catalog (may be null) 
		//2.TABLE_SCHEM String => table schema (may be null) 
		//3.TABLE_NAME String => table name 
		//4.COLUMN_NAME String => column name 
		//5.KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key). 
		//6.PK_NAME String => primary key name (may be null) 

		List<PKInfo> pks = getPrimaryKeys(statement, schema, table);
		Iterator<PKInfo> it = pks.iterator();
		
	    String catalog = statement.connection.getCatalog();

    CannedResultSet cr = new CannedResultSet(
                    "TABLE_CAT", DataType.text(),
                    "TABLE_SCHEM",       DataType.text(),
                    "TABLE_NAME",        DataType.text(),
                    "COLUMN_NAME",       DataType.text(),
                    "KEY_SEQ",           DataType.text(),
                    "PK_NAME",           DataType.text());

    // determine the schemas

    int seq = 0;
    while (it.hasNext())
    {
      PKInfo info = it.next();
      seq++;

      cr.addRow(catalog,         // TABLE_CAT
              info.schema,             // TABLE_SCHEM
              info.table,              // TABLE_NAME
              info.name,               // COLUMN_NAME
              seq,                     // KEY_SEQ
              null                     // PK_NAME
      );
    }


    return new CassandraResultSet(statement, cr);
}

	private class Entry
    {
    	static final String UTF8_TYPE = "UTF8Type";
        static final String ASCII_TYPE = "AsciiType";
        static final String INT32_TYPE = "Int32Type";
        static final String BOOLEAN_TYPE = "BooleanType";

        String name = null;
        ByteBuffer value = null;
        String type = null;
        
        private Entry(String name,ByteBuffer value,String type)
        {
            this.name = name;
            this.value = value;
            this.type = type;
        }        
    }
}
