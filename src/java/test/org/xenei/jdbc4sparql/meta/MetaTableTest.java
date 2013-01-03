package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.impl.TableDefImpl;
import org.xenei.jdbc4sparql.mock.MockSchema;

public class MetaTableTest
{
	private TableDefImpl def;
	private Object[][] rows;

	void compare( final Object[] expected, final ResultSet rs )
			throws SQLException
	{
		for (int i = 0; i < expected.length; i++)
		{
			final Object found = rs.getObject(i + 1);
			if (expected[i] == null)
			{
				Assert.assertNull(found);
			}
			else
			{
				Assert.assertNotNull(found);
				Assert.assertEquals(expected[i].getClass(), found.getClass());
				Assert.assertEquals(expected[i], found);
			}
		}
		try
		{
			rs.getObject(expected.length + 2);
			Assert.fail("Should have thrown exception");
		}
		catch (final SQLException e)
		{
			// do nothing
		}
	}

	@Before
	public void setUp()
	{
		def = new TableDefImpl(MetaNamespace.NS, "TestDef");
		def.add(MetaColumn.getStringInstance("NULLABLE_STRING").setNullable(
				DatabaseMetaData.columnNullable));
		def.add(MetaColumn.getStringInstance("STRING"));
		def.add(MetaColumn.getIntInstance("INT"));
		def.add(MetaColumn.getIntInstance("NULLABLE_INT").setNullable(
				DatabaseMetaData.columnNullable));
		rows = new Object[][] { new Object[] { "string", "row1", 5, 10 },
				new Object[] { null, "row2", 5, null }, };
	}

	@Test
	public void testSortedTable() throws Exception
	{
		def.addKey("STRING");
		final DataTable table = new DataTable(new MockSchema(), def);
		for (final Object[] row : rows)
		{
			table.addData(row);
		}
		table.addData(rows[0]);

		final ResultSet rs = table.getResultSet();
		Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
		rs.next();
		compare(rows[0], rs);

		rs.next();
		compare(rows[0], rs);

		rs.next();
		compare(rows[1], rs);

		Assert.assertFalse(rs.next());
	}

	@Test
	public void testTable() throws Exception
	{
		final DataTable table = new DataTable(new MockSchema(), def);
		for (final Object[] row : rows)
		{
			table.addData(row);
		}
		table.addData(rows[0]);
		final ResultSet rs = table.getResultSet();
		Assert.assertTrue(rs.isBeforeFirst());
		rs.next();
		compare(rows[0], rs);

		rs.next();
		compare(rows[1], rs);

		rs.next();
		compare(rows[0], rs);

		Assert.assertTrue(rs.isLast());
		rs.next();
		Assert.assertTrue(rs.isAfterLast());
	}

	@Test
	public void testUniqueSortedTable() throws Exception
	{
		def.addKey("STRING");
		def.setUnique();
		final DataTable table = new DataTable(new MockSchema(), def);
		for (final Object[] row : rows)
		{
			table.addData(row);
		}
		table.addData(rows[0]);

		final ResultSet rs = table.getResultSet();
		rs.first();
		compare(rows[0], rs);

		rs.next();
		compare(rows[1], rs);

		Assert.assertTrue(rs.isLast());
	}

}
