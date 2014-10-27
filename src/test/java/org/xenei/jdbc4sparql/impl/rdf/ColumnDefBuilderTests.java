package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.ColumnDef;

public class ColumnDefBuilderTests
{
	private Model model;

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
	public void testIntegerCreation()
	{
		final RdfColumnDef.Builder builder = RdfColumnDef.Builder
				.getIntegerBuilder();
		final ColumnDef cd = builder.build(model);

		Assert.assertEquals(false, cd.isAutoIncrement());
		Assert.assertEquals(false, cd.isCaseSensitive());
		Assert.assertEquals("", cd.getColumnClassName());
		Assert.assertEquals(false, cd.isCurrency());
		Assert.assertEquals(false, cd.isDefinitelyWritable());
		Assert.assertEquals(0, cd.getDisplaySize());
		Assert.assertEquals(0, cd.getNullable());
		Assert.assertEquals(0, cd.getPrecision());
		Assert.assertEquals(false, cd.isReadOnly());
		Assert.assertEquals(0, cd.getScale());
		Assert.assertEquals(false, cd.isSearchable());
		Assert.assertEquals(true, cd.isSigned());
		Assert.assertEquals("Integer", cd.getTypeName());
		Assert.assertEquals(false, cd.isWritable());
	}

	@Test
	public void testSetValuesCreation()
	{
		final RdfColumnDef.Builder builder = new RdfColumnDef.Builder();
		builder.setType(Types.VARCHAR).setAutoIncrement(true)
		.setCaseSensitive(true).setColumnClassName("foo")
		.setCurrency(true).setDefinitelyWritable(true)
		.setDisplaySize(5)
		.setNullable(DatabaseMetaData.columnNullableUnknown)
		.setPrecision(3).setReadOnly(true).setScale(10)
		.setSearchable(true).setSigned(true).setTypeName("bar")
		.setWritable(true);

		final ColumnDef cd = builder.build(model);

		Assert.assertEquals(true, cd.isAutoIncrement());
		Assert.assertEquals(true, cd.isCaseSensitive());
		Assert.assertEquals("foo", cd.getColumnClassName());
		Assert.assertEquals(true, cd.isCurrency());
		Assert.assertEquals(true, cd.isDefinitelyWritable());
		Assert.assertEquals(5, cd.getDisplaySize());
		Assert.assertEquals(DatabaseMetaData.columnNullableUnknown,
				cd.getNullable());
		Assert.assertEquals(3, cd.getPrecision());
		Assert.assertEquals(true, cd.isReadOnly());
		Assert.assertEquals(10, cd.getScale());
		Assert.assertEquals(true, cd.isSearchable());
		Assert.assertEquals(true, cd.isSigned());
		Assert.assertEquals("bar", cd.getTypeName());
		Assert.assertEquals(true, cd.isWritable());
	}

	@Test
	public void testSmallIntCreation()
	{
		final RdfColumnDef.Builder builder = RdfColumnDef.Builder
				.getSmallIntBuilder();
		final ColumnDef cd = builder.build(model);

		Assert.assertEquals(false, cd.isAutoIncrement());
		Assert.assertEquals(false, cd.isCaseSensitive());
		Assert.assertEquals("", cd.getColumnClassName());
		Assert.assertEquals(false, cd.isCurrency());
		Assert.assertEquals(false, cd.isDefinitelyWritable());
		Assert.assertEquals(0, cd.getDisplaySize());
		Assert.assertEquals(0, cd.getNullable());
		Assert.assertEquals(0, cd.getPrecision());
		Assert.assertEquals(false, cd.isReadOnly());
		Assert.assertEquals(0, cd.getScale());
		Assert.assertEquals(false, cd.isSearchable());
		Assert.assertEquals(true, cd.isSigned());
		Assert.assertEquals("Short", cd.getTypeName());
		Assert.assertEquals(false, cd.isWritable());
	}

	@Test
	public void testStandardCreation()
	{
		final RdfColumnDef.Builder builder = new RdfColumnDef.Builder();
		builder.setType(Types.VARCHAR);
		final ColumnDef cd = builder.build(model);

		Assert.assertEquals(false, cd.isAutoIncrement());
		Assert.assertEquals(false, cd.isCaseSensitive());
		Assert.assertEquals("", cd.getColumnClassName());
		Assert.assertEquals(false, cd.isCurrency());
		Assert.assertEquals(false, cd.isDefinitelyWritable());
		Assert.assertEquals(0, cd.getDisplaySize());
		Assert.assertEquals(0, cd.getNullable());
		Assert.assertEquals(0, cd.getPrecision());
		Assert.assertEquals(false, cd.isReadOnly());
		Assert.assertEquals(0, cd.getScale());
		Assert.assertEquals(false, cd.isSearchable());
		Assert.assertEquals(false, cd.isSigned());
		Assert.assertEquals("String", cd.getTypeName());
		Assert.assertEquals(false, cd.isWritable());
	}

	@Test
	public void testStringCreation()
	{
		final RdfColumnDef.Builder builder = RdfColumnDef.Builder
				.getStringBuilder();
		final ColumnDef cd = builder.build(model);

		Assert.assertEquals(false, cd.isAutoIncrement());
		Assert.assertEquals(false, cd.isCaseSensitive());
		Assert.assertEquals("", cd.getColumnClassName());
		Assert.assertEquals(false, cd.isCurrency());
		Assert.assertEquals(false, cd.isDefinitelyWritable());
		Assert.assertEquals(0, cd.getDisplaySize());
		Assert.assertEquals(0, cd.getNullable());
		Assert.assertEquals(0, cd.getPrecision());
		Assert.assertEquals(false, cd.isReadOnly());
		Assert.assertEquals(0, cd.getScale());
		Assert.assertEquals(false, cd.isSearchable());
		Assert.assertEquals(false, cd.isSigned());
		Assert.assertEquals("String", cd.getTypeName());
		Assert.assertEquals(false, cd.isWritable());
	}
}
