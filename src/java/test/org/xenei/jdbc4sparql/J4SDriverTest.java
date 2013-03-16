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

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
	
	
	@Before
	public void setup() throws Exception
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");
		
		fUrl = J4SDriverTest.class.getResource("./J4SDriverTest.ttl"); // /org/xenei/jdbc4sparql/J4SDriverTest.ttl");

		url = "jdbc:j4s?catalog=test&type=turtle:"
				+ fUrl.toString();

		conn = DriverManager.getConnection(url, "myschema", "mypassw");
	}

	@Test
	public void testTestDriverLoading() throws ClassNotFoundException,
			SQLException
	{
		


		// verify table exists
		final DatabaseMetaData metaData = conn.getMetaData();
		ResultSet rs = metaData.getTables("test", "", "fooTable", null);
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

	
	private List<String> getColumnNames( String table ) throws SQLException
	{
		ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), conn.getSchema(),
				table, null);
		final List<String> colNames = new ArrayList<String>();
		while (rs.next())
		{
			colNames.add(rs.getString(4));
		}
		return colNames;
	}
	
	
	
}
