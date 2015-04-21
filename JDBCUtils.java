/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for JDBC
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Atri Sharma <atri.jiit@gmail.com>
 * Changes by: Heimir Sverrisson <heimir.sverrisson@gmail.com>, 2015-04-17
 *
 * IDENTIFICATION
 *                jdbc2_fdw/JDBCUtils.java
 *
 *-------------------------------------------------------------------------
 */

import java.sql.*;
import java.text.*;
import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.MalformedURLException;
import java.util.*;
public class JDBCUtils
{
    private ResultSet               resultSet;
    private Connection              conn = null;
    private int                     numberOfColumns;
    private int                     numberOfRows;
    private Statement               stmt = null;
    private String[]                resultRow;
    private static JDBCDriverLoader jdbcDriverLoader;
    private StringWriter            exceptionStringWriter;
    private PrintWriter             exceptionPrintWriter;
    private int                     queryTimeoutValue;
    private ResultSetMetaData       rSetMetadata;

    /*
     * createConnection
     *      Initiates the connection to the foreign database after setting 
     *      up initial configuration.
     *      Caller will pass in a six element array with the following elements:
     *          0 - Driver class name, 1 - JDBC URL, 2 - Username
     *          3 - Password, 4 - Query timeout in seconds, 5 - jarfile
     *      Returns:
     *          null on success
     *          otherwise a string containing a stack trace
     */
    public String
    createConnection(String[] options) throws IOException
    {       
        DatabaseMetaData        dbMetadata;
        Properties              jdbcProperties;
        Class                   jdbcDriverClass = null;
        Driver                  jdbcDriver = null;
        String                  driverClassName = options[0];
        String                  url = options[1];
        String                  userName = options[2];
        String                  password = options[3];
        String                  qTimeoutValue = options[4];
        String                  fileName = options[5];

        queryTimeoutValue = Integer.parseInt(qTimeoutValue);
        exceptionStringWriter = new StringWriter();
        exceptionPrintWriter = new PrintWriter(exceptionStringWriter);
        numberOfColumns = 0;
        try {
            File JarFile = new File(fileName);
            String jarfile_path = JarFile.toURI().toURL().toString();
            if (jdbcDriverLoader == null) {
                /* If jdbcDriverLoader is being created. */
                jdbcDriverLoader = new JDBCDriverLoader(new URL[]{JarFile.toURI().toURL()}); 
            } else if (jdbcDriverLoader.CheckIfClassIsLoaded(driverClassName) == null) {
                jdbcDriverLoader.addPath(jarfile_path);
            }       
            jdbcDriverClass = jdbcDriverLoader.loadClass(driverClassName);
            jdbcDriver = (Driver)jdbcDriverClass.newInstance();
            jdbcProperties = new Properties();
            jdbcProperties.put("user", userName);
            jdbcProperties.put("password", password);
            conn = jdbcDriver.connect(url, jdbcProperties);
            dbMetadata = conn.getMetaData();
        } catch (Exception e) {
            /* If an exception occurs,it is returned back to the
             * calling C code by returning a Java String object
             * that has the exception's stack trace.
             * If all goes well,a null String is returned. */
            e.printStackTrace(exceptionPrintWriter);
            return (new String(exceptionStringWriter.toString()));
        }
        return null;
    }

    /*
     * createStatement
     *      Create a statement object based on the query
     *      Returns:
     *          null on success
     *          otherwise a string containing a stack trace
     */
    public String
    createStatement(String query) throws IOException
    {
        try {
            if(conn == null){
                throw new Exception("Must create connection before creating a statment");
            }
            if(stmt != null){
                throw new Exception("Must close a prior statement before creating a new one");
            }
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (queryTimeoutValue != 0) {
                stmt.setQueryTimeout(queryTimeoutValue);
            }
            resultSet = stmt.executeQuery(query);
            rSetMetadata = resultSet.getMetaData();
            numberOfColumns = rSetMetadata.getColumnCount();
            resultRow = new String[numberOfColumns];
        } catch (Exception e) {
            /* If an exception occurs,it is returned back to the
             * calling C code by returning a Java String object
             * that has the exception's stack trace.
             * If all goes well,a null String is returned. */
            e.printStackTrace(exceptionPrintWriter);
            return (new String(exceptionStringWriter.toString()));
        }
        return null;
    }

    /*
     * returnResultSet
     *      Returns the result set that is returned from the foreign database
     *      after execution of the query to C code. One row is returned at a time
     *      as a String array. After last row null is returned.
     */
    public String[] 
    returnResultSet()
    {
        int i = 0;
        try {
            /* Row-by-row processing is done in jdbc_fdw.One row
             * at a time is returned to the C code. */
            if (resultSet.next()) {
                for (i = 0; i < numberOfColumns; i++) {
                    resultRow[i] = resultSet.getString(i+1); // Convert all columns to String
                }
                ++numberOfRows;                         
                /* The current row in resultSet is returned
                 * to the C code in a Java String array that
                 * has the value of the fields of the current
                 * row as it values. */
                return (resultRow);
            }
        } catch (Exception e) {
            e.printStackTrace(); // Best we can do, cannot return exception to caller!
        }
        /* All of resultSet's rows have been returned to the C code. */
        return null;
    }

    /*
     * closeStatement
     *      Releases the resources used by statement. Keeps the connection
     *      open for another statement to be executed.             
     */
    public String 
    closeStatement()
    {
        try {
            if(resultSet != null){
                resultSet.close();
                resultSet = null;
            }
            if(stmt != null){
                stmt.close();
                stmt = null;
            }
            resultRow = null;
        } catch (Exception e) {
            /* If an exception occurs,it is returned back to the
             * calling C code by returning a Java String object
             * that has the exception's stack trace.
             * If all goes well,a null String is returned. */
            e.printStackTrace(exceptionPrintWriter);
            return (new String(exceptionStringWriter.toString()));
        }
        return null;
    }

    /*
     * closeConnection
     *     Releases the resources used by connection.
     */
    public String 
    closeConnection()
    {
        closeStatement(); // For good measure
        try {
            if(conn != null){
                conn.close();
                conn = null;
            }
        } catch (Exception e) {
            /* If an exception occurs,it is returned back to the
             * calling C code by returning a Java String object
             * that has the exception's stack trace.
             * If all goes well,a null String is returned. */
            e.printStackTrace(exceptionPrintWriter);
            return (new String(exceptionStringWriter.toString()));
        }
        return null;
    }

    /*
     * cancel
     *      Cancels the query and releases the resources in case query
     *      cancellation is requested by the user.
     *      Returns:
     *          null on success
     *          otherwise a string containing a stack trace
     */
    public String 
    cancel()
    {
        return closeStatement();
    }

    /*
     * getNumberOfColumns: A simple getter for the field numberOfColumns
     */
    public int 
    getNumberOfColumns()
    {
        return numberOfColumns;
    }
}
