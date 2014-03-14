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

//import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class MetadataIntrospectTest
{
    private static final String HOST = "localhost";
    private static final int PORT = 9042;

    private static java.sql.Connection con = null;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?version=3.0.0",HOST,PORT,"system");
        System.out.println("Connection URL = '"+URL +"'");
        
        con = DriverManager.getConnection(URL);

    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
    }


  @Test
  public void testPrintAllMethods() throws SQLException
  {
    DatabaseMetaData metaData = con.getMetaData();

    Class<?> mt = metaData.getClass();

        for (Method method : mt.getDeclaredMethods()) {
          Class<?> returnType = method.getReturnType();
          System.out.println(returnType.getSimpleName() + "\t " + method.getName());
        }
   }

    @Test
    public void testCallAllSimpleMethods() throws SQLException
    {
        DatabaseMetaData metaData = con.getMetaData();

        Class<?> mt = metaData.getClass();

        System.out.println("Simple Returns\n");
        for (Method method : mt.getDeclaredMethods()) {
          Class<?> returnType = method.getReturnType();

          if ((returnType.isPrimitive() || returnType == String.class) && method.getParameterTypes().length == 0) {
            try {
              System.out.println(method.getName() + ":\t " + method.invoke(metaData));
            } catch (IllegalAccessException e) {
              e.printStackTrace();
            } catch (InvocationTargetException e) {
              e.printStackTrace();
            }  catch (IllegalArgumentException e) {
              System.out.println("Failed Method:  " + method.getName());
              throw e;
            }
          }
        }
    }

  @Test
  public void testUsePropertiesMethods() throws Exception
  {
    DatabaseMetaData metaData = con.getMetaData();

    Class<?> mt = metaData.getClass();
    BeanInfo bi = Introspector.getBeanInfo(mt);

    for (PropertyDescriptor pd : bi.getPropertyDescriptors()) {
      System.out.println(pd.getName() + ": \t " + pd.getReadMethod().invoke(metaData));
    }
  }

  @Test
  public void testUsePropertiesMethodsWithInterface() throws Exception
  {
    DatabaseMetaData metaData = con.getMetaData();

    Class<?> mt = DatabaseMetaData.class;
    BeanInfo bi = Introspector.getBeanInfo(mt);

    for (PropertyDescriptor pd : bi.getPropertyDescriptors()) {
      System.out.println(pd.getName() + ": \t " + pd.getReadMethod().invoke(metaData));
    }
  }

  @Test
  public void testTheirProperties() throws Exception {

    DatabaseMetaData dmd = con.getMetaData();

    Object[] defaultArg = new Object[]{};

    Map<Object, Object> properties = new HashMap<Object, Object>();

    String STRING = "String";
    String GET = "get";

    Class<?> metaClass = dmd.getClass();
    Method[] metaMethods = metaClass.getMethods();

    for (int i = 0; i < metaMethods.length; i++) {

      Class<?> clazz = metaMethods[i].getReturnType();

      String methodName = metaMethods[i].getName();

      if (methodName == null || clazz == null) {

        continue;
      }

      if (clazz.isPrimitive() || clazz.getName().endsWith(STRING)) {

        if (methodName.startsWith(GET)) {

          methodName = methodName.substring(3);
        }

        try {

          Object res = metaMethods[i].invoke(dmd, defaultArg);
          if (res != null) {

            properties.put(methodName, res.toString());
          }

        } catch (AbstractMethodError e) {

          continue;

        } catch (IllegalArgumentException e) {

          continue;

        } catch (IllegalAccessException e) {

          continue;

        } catch (InvocationTargetException e) {

          continue;
        }

      }

    }

    for (Object p : properties.keySet()) {
      System.out.println(p + ": " + properties.get(p));

    }
    return ;
  }

}
