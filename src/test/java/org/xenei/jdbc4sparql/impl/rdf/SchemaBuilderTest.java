package org.xenei.jdbc4sparql.impl.rdf;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class SchemaBuilderTest {

	private Model model;
	private EntityManager mgr;
	private RdfTable.Builder tableBldr;
	private RdfCatalog mockCatalog;

	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		mgr = new EntityManagerImpl( model );
		final RdfTableDef.Builder builder = new RdfTableDef.Builder()
				.addColumnDef(
						RdfColumnDef.Builder.getStringBuilder().build( mgr ))
				.addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build( mgr ));
		tableBldr = new RdfTable.Builder().setName("testTable")
				.setTableDef(builder.build( mgr )).setColumn(0, "StringCol")
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

		final RdfSchema schema = builder.build( mgr );

		tableBldr.setSchema(schema).setType("test table").build( mgr );

		schema.getTables();

		Assert.assertEquals("schema", schema.getName().getShortName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(1, schema.getTables().size());
	}

	@Test
	public void testAddTableTableRead() {
		final RdfSchema.Builder builder = new RdfSchema.Builder().setName(
				"schema").setCatalog(mockCatalog);

		final RdfSchema schema = builder.build( mgr );
		Assert.assertEquals(0, schema.getTables().size());

		tableBldr.setSchema(schema).setType("Test table").build( mgr );

		Assert.assertEquals("schema", schema.getName().getShortName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(1, schema.getTables().size());
	}

	@Test
	public void testDefault() {
		final RdfSchema.Builder builder = new RdfSchema.Builder().setName(
				"schema").setCatalog(mockCatalog);

		final Schema schema = builder.build( mgr );
		schema.getTables();
		Assert.assertEquals("schema", schema.getName().getShortName());
		Assert.assertNotNull(schema.getTables());
		Assert.assertEquals(0, schema.getTables().size());
	}
}
