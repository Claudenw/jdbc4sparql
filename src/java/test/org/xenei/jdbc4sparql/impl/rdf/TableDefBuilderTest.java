package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.TableDef;

public class TableDefBuilderTest
{
	private Model model;

	private Key getPrimaryKey()
	{
		new Builder();
		final KeyBuilder builder = new KeyBuilder()
				.addSegment(new Builder().build(model))
				.setUnique(true).setKeyName("PK");
		return builder.build(model);
	}

	private Key getSortKey()
	{
		new Builder();
		final KeyBuilder builder = new KeyBuilder()
				.addSegment(new Builder().setIdx(1).build(model))
				.setUnique(false).setKeyName("SK");
		return builder.build(model);
	}

	@Before
	public void setUp() throws Exception
	{
		model = ModelFactory.createDefaultModel();
	}

	@After
	public void tearDown() throws Exception
	{
		model.close();
	}

	@Test
	public void testBadPrimaryKey()
	{
		try
		{
			new TableDefBuilder()
					.addColumnDef(
							Builder.getStringBuilder().build(model))
					.addColumnDef(
							Builder.getIntegerBuilder().build(model))
					.setPrimaryKey(getSortKey());
			Assert.fail("Should have thrown IllegalArgumentException");
		}
		catch (final IllegalArgumentException e)
		{
			// expected
		}
	}

	@Test
	public void testDefault()
	{
		final TableDefBuilder builder = new TableDefBuilder()
				.addColumnDef(Builder.getStringBuilder().build(model))
				.addColumnDef(Builder.getIntegerBuilder().build(model));
		final TableDef tableDef = builder.build(model);

		Assert.assertEquals(2, tableDef.getColumnCount());
		Assert.assertEquals(Builder.getStringBuilder().build(model),
				tableDef.getColumnDef(0));
		Assert.assertEquals(Builder.getIntegerBuilder().build(model),
				tableDef.getColumnDef(1));
		Assert.assertEquals(1, tableDef.getColumnIndex(Builder
				.getIntegerBuilder().build(model)));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertNull(tableDef.getPrimaryKey());
		Assert.assertNull(tableDef.getSortKey());
		Assert.assertNull(tableDef.getSuperTableDef());
		
		model.write( System.out, "TURTLE" );
	}

	@Test
	public void testPrimaryKey()
	{
		final TableDefBuilder builder = new TableDefBuilder()
				.addColumnDef(Builder.getStringBuilder().build(model))
				.addColumnDef(Builder.getIntegerBuilder().build(model))
				.setPrimaryKey(getPrimaryKey());
		final TableDef tableDef = builder.build(model);

		Assert.assertEquals(2, tableDef.getColumnCount());
		Assert.assertEquals(Builder.getStringBuilder().build(model),
				tableDef.getColumnDef(0));
		Assert.assertEquals(Builder.getIntegerBuilder().build(model),
				tableDef.getColumnDef(1));
		Assert.assertEquals(1, tableDef.getColumnIndex(Builder
				.getIntegerBuilder().build(model)));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertEquals(getPrimaryKey(), tableDef.getPrimaryKey());
		Assert.assertNull(tableDef.getSortKey());
		Assert.assertNull(tableDef.getSuperTableDef());
	}

	@Test
	public void testSortKey()
	{
		final TableDefBuilder builder = new TableDefBuilder()
				.addColumnDef(Builder.getStringBuilder().build(model))
				.addColumnDef(Builder.getIntegerBuilder().build(model))
				.setSortKey(getSortKey());
		final TableDef tableDef = builder.build(model);

		Assert.assertEquals(2, tableDef.getColumnCount());
		Assert.assertEquals(Builder.getStringBuilder().build(model),
				tableDef.getColumnDef(0));
		Assert.assertEquals(Builder.getIntegerBuilder().build(model),
				tableDef.getColumnDef(1));
		Assert.assertEquals(1, tableDef.getColumnIndex(Builder
				.getIntegerBuilder().build(model)));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertNull(tableDef.getPrimaryKey());
		Assert.assertEquals(getSortKey(), tableDef.getSortKey());
		Assert.assertNull(tableDef.getSuperTableDef());
	}

	@Test
	public void testSuperTable()
	{
		TableDefBuilder builder = new TableDefBuilder()
				.addColumnDef(Builder.getStringBuilder().build(model))
				.addColumnDef(Builder.getIntegerBuilder().build(model));
		final TableDef tableDef2 = builder.build(model);

		builder = new TableDefBuilder()
				.addColumnDef(Builder.getStringBuilder().build(model))
				.addColumnDef(Builder.getIntegerBuilder().build(model))
				.setSuperTableDef(tableDef2);
		final TableDef tableDef = builder.build(model);

		Assert.assertEquals(2, tableDef.getColumnCount());
		Assert.assertEquals(Builder.getStringBuilder().build(model),
				tableDef.getColumnDef(0));
		Assert.assertEquals(Builder.getIntegerBuilder().build(model),
				tableDef.getColumnDef(1));
		Assert.assertEquals(1, tableDef.getColumnIndex(Builder
				.getIntegerBuilder().build(model)));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertNull(tableDef.getPrimaryKey());
		Assert.assertNull(tableDef.getSortKey());
		Assert.assertEquals(tableDef2, tableDef.getSuperTableDef());
	}
}
