package org.xenei.jdbc4sparql.impl.rdf;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Schema;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class SchemaBuilderTest {

	private Model model;
	private RdfTable.Builder tableBldr;
	private RdfCatalog mockCatalog;

	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		final RdfTableDef.Builder builder = new RdfTableDef.Builder()
				.addColumnDef(
						RdfColumnDef.Builder.getStringBuilder().build(model))
				.addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build(model));
		tableBldr = new RdfTable.Builder().setName("testTable")
				.setTableDef(builder.build(model)).setColumn(0, "StringCol")
				.setColumn(1, "IntCol");
		mockCatalog = mock(RdfCatalog.class);
		when(mockCatalog.getResource()).thenReturn(
				model.createResource("http://example.com/mockCatalog"));
		when(mockCatalog.getShortName()).thenReturn("mockCatalog");
	}

	@After
	public void tearDown() throws Exception {
		model.close();
	}

	@Test
	public void testAddTable() throws Exception {
		final RdfSchema.Builder builder = new RdfSchema.Builder().setName(
				"schema").setCatalog(mockCatalog);

		final RdfSchema schema = builder.build(model);

		tableBldr.setSchema(schema).setType("test table").build(model);

		schema.getTables();

		Assert.assertEquals("schema", schema.getName().getShortName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(1, schema.getTables().size());
	}

	@Test
	public void testAddTableTableRead() {
		final RdfSchema.Builder builder = new RdfSchema.Builder().setName(
				"schema").setCatalog(mockCatalog);

		final RdfSchema schema = builder.build(model);
		Assert.assertEquals(0, schema.getTables().size());

		tableBldr.setSchema(schema).setType("Test table").build(model);

		Assert.assertEquals("schema", schema.getName().getShortName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(1, schema.getTables().size());
	}

	@Test
	public void testDefault() {
		final RdfSchema.Builder builder = new RdfSchema.Builder().setName(
				"schema").setCatalog(mockCatalog);

		final Schema schema = builder.build(model);
		schema.getTables();
		Assert.assertEquals("schema", schema.getName().getShortName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(0, schema.getTables().size());
	}
}
