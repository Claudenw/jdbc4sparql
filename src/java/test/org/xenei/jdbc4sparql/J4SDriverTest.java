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

import org.junit.Assert;
import org.junit.Test;

public class J4SDriverTest
{

	@Test
	public void testTestDriverLoading() throws ClassNotFoundException,
			SQLException
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		final URL fUrl = J4SDriverTest.class.getResource("./J4SDriverTest.ttl"); // /org/xenei/jdbc4sparql/J4SDriverTest.ttl");

		final String url = "jdbc:j4s?catalog=test&type=turtle:"
				+ fUrl.toString();

		final Connection conn = DriverManager.getConnection(url, "myschema",
				"mypassw");

		// verify table exists
		final DatabaseMetaData metaData = conn.getMetaData();
		ResultSet rs = metaData.getTables("test", "", "fooTable", null);
		Assert.assertTrue(rs.next());

		// get the column names.
		rs = metaData.getColumns(conn.getCatalog(), conn.getSchema(),
				"fooTable", null);
		final List<String> colNames = new ArrayList<String>();
		while (rs.next())
		{
			colNames.add(rs.getString(4));
		}

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
