package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class J4SStatementOuterJoinTest
{
	// file URL
	private URL fUrl;

	// J4SUrl
	private String url;

	// JDBC Connection
	private Connection conn;

	private Statement stmt;

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

		fUrl = J4SDriverTest.class
				.getResource("./J4SStatementOuterJoinTest.ttl"); // /org/xenei/jdbc4sparql/J4SDriverTest.ttl");

		url = "jdbc:j4s?catalog=test&type=turtle&builder=org.xenei.jdbc4sparql.sparql.builders.SimpleNullableBuilder:"
				+ fUrl.toString();

		conn = DriverManager.getConnection(url, "myschema", "mypassw");
		conn.setAutoCommit(false);
		stmt = conn.createStatement();
	}

	@After
	public void tearDown()
	{
		try
		{
			stmt.close();
		}
		catch (final SQLException ignore)
		{
		}
		try
		{
			conn.close();
		}
		catch (final SQLException ignore)
		{
		}
	}

	@Test
	public void testLeftOuterJoin() throws ClassNotFoundException, SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable left outer join barTable on fooTable.IntCol=barTable.IntCol");
		try
		{
			boolean foundNull = false;
			while (rset.next())
			{
				rset.getString(2);
				foundNull |= rset.wasNull();
			}
			Assert.assertTrue( "did not find null", foundNull);
		}
		finally
		{
			rset.close();
		}
	}

	@Test
	public void testOuterJoin() throws ClassNotFoundException, SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable outer join barTable on fooTable.IntCol=barTable.IntCol");
		try
		{
			boolean foundNull = false;
			while (rset.next())
			{
				rset.getString(2);
				foundNull |= rset.wasNull();
			}
			Assert.assertTrue( "did not find null", foundNull);
		}
		finally
		{
			rset.close();
		}
	}
	
	@Test
	@Ignore
	public void testRightOuterJoin() throws ClassNotFoundException, SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable right outer join barTable on fooTable.IntCol=barTable.IntCol");
		try
		{
			boolean foundNull = false;
			while (rset.next())
			{
				rset.getString(2);
				foundNull |= rset.wasNull();
			}
			Assert.assertTrue( "did not find null", foundNull);
		}
		finally
		{
			rset.close();
		}
	}
	
	@Test
	@Ignore
	public void testFullOuterJoin() throws ClassNotFoundException, SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable right outer join barTable on fooTable.IntCol=barTable.IntCol");
		try
		{
			boolean foundNull = false;
			while (rset.next())
			{
				rset.getString(2);
				foundNull |= rset.wasNull();
			}
			Assert.assertTrue( "did not find null", foundNull);
		}
		finally
		{
			rset.close();
		}
	}

}
