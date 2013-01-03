package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.impl.TableDefImpl;

public class MetaTableDefTest
{
	private TableDefImpl def;

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
	}

	@Test
	public void testAddColumns()
	{
		Assert.assertEquals(4, def.getColumnCount());
		Assert.assertEquals("NULLABLE_STRING", def.getColumnDef(0).getLabel());
		Assert.assertEquals("STRING", def.getColumnDef(1).getLabel());
		Assert.assertEquals("INT", def.getColumnDef(2).getLabel());
		Assert.assertEquals("NULLABLE_INT", def.getColumnDef(3).getLabel());
		Assert.assertNull(def.getSortKey());
	}

	@Test
	public void testAddSortKey()
	{
		def.addKey("STRING");
		Assert.assertNotNull(def.getSortKey());
		Assert.assertFalse(def.getSortKey().isUnique());
		def.setUnique();
		Assert.assertTrue(def.getSortKey().isUnique());
	}

	@Test
	public void testGetColumn()
	{
		final ColumnDef col = def.getColumnDef("STRING");
		Assert.assertEquals(def.getColumnDef(1), col);
	}

	@Test
	public void testVerify()
	{
		Object[] row = new Object[] { "string1", "string", 5, 10 };
		def.verify(row);
		row = new Object[] { null, "string", 5, null };
		def.verify(row);
		row = new Object[] { null, "string", 5L, null };
		try
		{
			def.verify(row);
		}
		catch (final IllegalArgumentException expected)
		{ // do nothing
		}
		row = new Object[] { "string", null, 5, null };
		try
		{
			def.verify(row);
		}
		catch (final IllegalArgumentException expected)
		{ // do nothing
		}
	}

}
