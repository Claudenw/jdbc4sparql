package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;

public class SchemaBuilderTest
{

	private Model model;
	private TableBuilder tableBldr;
	private Catalog mockCatalog;

	@Before
	public void setUp() throws Exception
	{
		model = ModelFactory.createDefaultModel();
		final TableDefBuilder builder = new TableDefBuilder().addColumnDef(
				ColumnDefBuilder.getStringBuilder().build(model)).addColumnDef(
				ColumnDefBuilder.getIntegerBuilder().build(model));
		tableBldr = new TableBuilder().setName("testTable")
				.setTableDef(builder.build(model)).setColumn(0, "StringCol")
				.setColumn(1, "IntCol");
		mockCatalog = Mockito.mock(Catalog.class);
		Mockito.when(mockCatalog.getResource()).thenReturn(
				model.createResource("http://example.com/mockCatalog"));
	}

	@After
	public void tearDown() throws Exception
	{
		model.close();
	}

	@Test
	public void testAddTable() throws Exception
	{
		final SchemaBuilder builder = new SchemaBuilder().setName("schema")
				.setCatalog(mockCatalog);

		final Schema schema = builder.build(model);

		tableBldr.setSchema(schema).setType("test table").build(model);

		schema.getTables();

		Assert.assertEquals("schema", schema.getName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(1, schema.getTables().size());
	}

	@Test
	public void testAddTableTableRead()
	{
		final SchemaBuilder builder = new SchemaBuilder().setName("schema")
				.setCatalog(mockCatalog);

		final Schema schema = builder.build(model);
		Assert.assertEquals(0, schema.getTables().size());

		tableBldr.setSchema(schema).setType("Test table").build(model);

		Assert.assertEquals("schema", schema.getName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(1, schema.getTables().size());
	}

	@Test
	public void testDefault()
	{
		final SchemaBuilder builder = new SchemaBuilder().setName("schema")
				.setCatalog(mockCatalog);

		final Schema schema = builder.build(model);
		schema.getTables();
		Assert.assertEquals("schema", schema.getName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(0, schema.getTables().size());
	}
}
