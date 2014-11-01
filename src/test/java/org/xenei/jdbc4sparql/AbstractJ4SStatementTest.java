package org.xenei.jdbc4sparql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;

public abstract class AbstractJ4SStatementTest extends AbstractJ4SSetup
{

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
	public void testMetadataQuery() throws Exception
	{
		conn.setCatalog(MetaCatalogBuilder.LOCAL_NAME);
		stmt.close();
		stmt = conn.createStatement();
		final ResultSet rset = stmt
				.executeQuery("select tbl.* from Tables tbl");
		while (rset.next())
		{
			rset.getString(1); // force a read.
		}
		rset.close();
	}

	/*
	 * SELECT tbl.* FROM Online_Account AS tbl
	 */
	@Test
	public void testSelectAllTableAlias() throws Exception
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
		final ResultSet rset = stmt
				.executeQuery("select tbl.* from fooTable tbl");
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
	public void testTableAlias() throws ClassNotFoundException, SQLException
	{
		final ResultSet rset = stmt
				.executeQuery("select IntCol from fooTable tbl where StringCol='Foo2String'");
		int i = 0;
		while (rset.next())
		{
			Assert.assertEquals(5, rset.getInt("IntCol"));
			new StringBuilder();
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
