/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class J4SDriverTest
{
	// file URL
	private URL fUrl;

	// J4SUrl
	private String url;

	// JDBC Connection
	private Connection conn;

	private List<String> getColumnNames( final String table )
			throws SQLException
			{
		final ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(),
				conn.getSchema(), table, null);
		final List<String> colNames = new ArrayList<String>();
		while (rs.next())
		{
			colNames.add(rs.getString(4));
		}
		return colNames;
			}

	@Before
	public void setup() throws Exception
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");
		fUrl = J4SDriverTest.class.getResource("./J4SDriverTest.ttl");
		url = "jdbc:j4s?catalog=test&type=turtle&builder:" + fUrl.toString();
	}

	@After
	public void tearDown() throws SQLException
	{
		conn.close();
	}

	@Test
	public void testTestDriverLoadingIdPwd() throws ClassNotFoundException,
	SQLException
	{
		conn = DriverManager.getConnection(url, "myschema", "mypassw");
		verifyCorrect();
	}

	@Test
	public void testTestDriverLoadingMemDatasetProducer()
			throws ClassNotFoundException, SQLException
	{
		final Properties properties = new Properties();
		properties.setProperty("DatasetProducer",
				"org.xenei.jdbc4sparql.config.MemDatasetProducer");
		conn = DriverManager.getConnection(url, properties);
		verifyCorrect();
	}

	@Test
	public void testTestDriverLoadingNoProps() throws ClassNotFoundException,
	SQLException
	{
		conn = DriverManager.getConnection(url);
		verifyCorrect();
	}

	@Test
	public void testTestDriverLoadingTDBDatasetProducer()
			throws ClassNotFoundException, SQLException
	{
		final Properties properties = new Properties();
		properties.setProperty("DatasetProducer",
				"org.xenei.jdbc4sparql.config.TDBDatasetProducer");
		conn = DriverManager.getConnection(url, properties);
		verifyCorrect();
	}

	private void verifyCorrect() throws SQLException
	{
		final DatabaseMetaData metaData = conn.getMetaData();

		// verify table exists
		final ResultSet rs1 = metaData.getTables(null, null, null, null);
		while (rs1.next())
		{
			System.out.println(String.format("cat: %s, schem: %s, tbl: %s",
					rs1.getString(1), rs1.getString(2), rs1.getString(3)));
		}

		final ResultSet rs = metaData.getTables("test", null, "fooTable", null);
		Assert.assertTrue(rs.next());

		// get the column names.

		final List<String> colNames = getColumnNames("fooTable");

		// execute a query against the table and verify results
		conn.setAutoCommit(false);
		final Statement stmt = conn.createStatement();
		final ResultSet rset = stmt.executeQuery("select * from fooTable");
		while (rset.next())
		{
			// verify that all colums are returned ... we don't care about value
			for (final String colName : colNames)
			{
				rset.getString(colName);
			}
		}
		stmt.close();
	}

}
