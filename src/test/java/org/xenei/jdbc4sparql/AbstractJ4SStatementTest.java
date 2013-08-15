package org.xenei.jdbc4sparql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractJ4SStatementTest
{

	// JDBC Connection
	protected Connection conn;

	protected Statement stmt;

	protected List<String> getColumnNames( final String table )
			throws SQLException
	{
		final ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(),
				conn.getSchema(), table, null);
		final List<String> colNames = new ArrayList<String>();
		while (rs.next())
		{
			// TODO remove this
			System.out.println(String.format("%s %s %s %s", rs.getString(1),
					rs.getString(2), rs.getString(3), rs.getString(4)));
			colNames.add(rs.getString(4));
		}
		return colNames;
	}

	@After
	public void tearDown()
	{
		try
		{
			if (stmt != null)
			{
				stmt.close();
			}
		}
		catch (final SQLException ignore)
		{
		}
		try
		{
			if (conn != null)
			{
				conn.close();
			}
		}
		catch (final SQLException ignore)
		{
		}
	}

	@Test
	public void testBadValueInEqualsConst() throws ClassNotFoundException,
			SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable inner, barTable where fooTable.StringCol=barTable.StringCol and fooTable.IntCol='Foo3String'");
		try
		{
			Assert.assertFalse("Should not return any values", rset.next());
		}
		finally
		{
			rset.close();
		}
	}

	@Test
	public void testColumnEqualConst() throws ClassNotFoundException,
			SQLException
	{
		final List<String> colNames = getColumnNames("fooTable");
		final ResultSet rset = stmt
				.executeQuery("select * from fooTable where StringCol='Foo2String'");
		int i = 0;
		while (rset.next())
		{
			final StringBuilder sb = new StringBuilder();
			for (final String colName : colNames)
			{
				sb.append(String.format("[%s]=%s ", colName,
						rset.getString(colName)));
			}
			final String s = sb.toString();
			Assert.assertTrue(s.contains("[StringCol]=Foo2String"));
			Assert.assertTrue(s.contains("[IntCol]=5"));
			Assert.assertTrue(s
					.contains("[type]=http://example.com/jdbc4sparql#fooTable"));
			Assert.assertTrue(s.contains("[NullableStringCol]=null"));
			Assert.assertTrue(s.contains("[NullableIntCol]=null"));
			i++;
		}
		Assert.assertEquals(1, i);
		rset.close();
		stmt.close();
	}

	@Test
	public void testFullRetrieval() throws ClassNotFoundException, SQLException
	{
		final String[][] results = {
				{ "[StringCol]=FooString",
						"[NullableStringCol]=FooNullableFooString",
						"[NullableIntCol]=6", "[IntCol]=5",
						"[type]=http://example.com/jdbc4sparql#fooTable" },
				{ "[StringCol]=Foo2String", "[NullableStringCol]=null",
						"[NullableIntCol]=null", "[IntCol]=5",
						"[type]=http://example.com/jdbc4sparql#fooTable" } };

		// get the column names.
		final List<String> colNames = getColumnNames("fooTable");
		final ResultSet rset = stmt.executeQuery("select * from fooTable");
		int i = 0;
		while (rset.next())
		{
			final List<String> lst = Arrays.asList(results[i]);
			for (final String colName : colNames)
			{
				lst.contains(String.format("[%s]=%s", colName,
						rset.getString(colName)));
			}
			i++;
		}
		Assert.assertEquals(2, i);
		rset.close();

	}

	@Test
	public void testInnerJoinSelect() throws SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol, fooTable.StringCol, barTable.StringCol from fooTable inner join barTable ON fooTable.StringCol=barTable.StringCol");

		while (rset.next())
		{
			Assert.assertEquals(5, rset.getInt(1));
			Assert.assertEquals(15, rset.getInt(2));
		}
		rset.close();
	}

	@Test
	public void testJoinSelect() throws SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable join barTable ON fooTable.StringCol=barTable.StringCol");

		while (rset.next())
		{
			Assert.assertEquals(5, rset.getInt(1));
			Assert.assertEquals(15, rset.getInt(2));
		}
		rset.close();
	}

	@Test
	public void testTableAlias() throws ClassNotFoundException, SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select IntCol from fooTable tbl where StringCol='Foo2String'");
		int i = 0;
		while (rset.next())
		{
			Assert.assertEquals( 5, rset.getInt("IntCol"));
			final StringBuilder sb = new StringBuilder();
			i++;
		}
		Assert.assertEquals(1, i);
		rset.close();
		stmt.close();
	}

	@Test
	public void testWhereEqualitySelect() throws SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable, barTable WHERE fooTable.StringCol=barTable.StringCol");

		while (rset.next())
		{
			Assert.assertEquals(5, rset.getInt(1));
			Assert.assertEquals(15, rset.getInt(2));
		}
		rset.close();
	}

}
