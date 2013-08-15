package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Level;
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

	@Before
	public void setup() throws Exception
	{
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("com.hp.hpl.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.DEBUG);
		
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
	@Ignore( "Full Outer Join not suported." )
	public void testFullOuterJoin() throws ClassNotFoundException, SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable full outer join barTable on fooTable.IntCol=barTable.IntCol");
		try
		{
			boolean foundNull = false;
			while (rset.next())
			{
				rset.getString(2);
				foundNull |= rset.wasNull();
			}
			Assert.assertTrue("did not find null", foundNull);
		}
		finally
		{
			rset.close();
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
			Assert.assertTrue("did not find null", foundNull);
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
			Assert.assertTrue("did not find null", foundNull);
		}
		finally
		{
			rset.close();
		}
	}

	@Test
	@Ignore( "Right Outer Join not suported." )
	public void testRightOuterJoin() throws ClassNotFoundException,
			SQLException
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
			Assert.assertTrue("did not find null", foundNull);
		}
		finally
		{
			rset.close();
		}
	}

}
