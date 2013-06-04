package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;

public class CatalogBuilderTest
{

	private Model model;
	private SchemaBuilder schemaBldr;

	@Before
	public void setUp() throws Exception
	{
		model = ModelFactory.createDefaultModel();

		schemaBldr = new SchemaBuilder().setName("testSchema");
	}

	@After
	public void tearDown() throws Exception
	{
		model.close();
	}

	@Test
	public void testAddSchema()
	{
		final Builder builder = new Builder().setName("catalog");

		final Catalog catalog = builder.build(model);

		schemaBldr.setCatalog(catalog);

		final Schema schema = schemaBldr.build(model);

		catalog.getSchemas();
		Assert.assertEquals("catalog", catalog.getName());
		Assert.assertNotNull(catalog.getSchemas());
		Assert.assertEquals(1, catalog.getSchemas().size());
	}

	@Test
	public void testAddTableTableRead()
	{
		final Builder builder = new Builder().setName("catalog");

		final Catalog catalog = builder.build(model);
		Assert.assertEquals(0, catalog.getSchemas().size());

		schemaBldr.setCatalog(catalog);

		final Schema schema = schemaBldr.build(model);

		Assert.assertEquals("catalog", catalog.getName());
		Assert.assertNotNull(catalog.getSchemas());
		Assert.assertEquals(1, catalog.getSchemas().size());
	}

	@Test
	public void testDefault()
	{
		final Builder builder = new Builder().setName("catalog");

		final Catalog catalog = builder.build(model);
		catalog.getSchemas();
		Assert.assertEquals("catalog", catalog.getName());
		Assert.assertNotNull(catalog.getSchemas());
		Assert.assertEquals(0, catalog.getSchemas().size());
	}

	@Test
	public void testFindSchema()
	{
		final Builder builder = new Builder().setName("catalog");

		final Catalog catalog = builder.build(model);

		schemaBldr.setCatalog(catalog).build(model);

		Assert.assertNotNull(catalog.findSchemas("testSchema"));

		Assert.assertNotNull(catalog.findSchemas(null));
	}

	@Test
	public void testGetSchema() throws Exception
	{
		final Builder builder = new Builder().setName("catalog");

		final Catalog catalog = builder.build(model);

		schemaBldr.setCatalog(catalog).build(model);

		model.write(System.out, "TURTLE");
		model.write(new FileOutputStream(new File("dump.ttl")), "TURTLE");

		Assert.assertNotNull(catalog.getSchema("testSchema"));

	}

	@Test
	public void testGetSchemas()
	{
		final Builder builder = new Builder().setName("catalog");

		final Catalog catalog = builder.build(model);

		schemaBldr.setCatalog(catalog);

		final Schema schema = schemaBldr.build(model);

		final Set<Schema> schemas = catalog.getSchemas();
		Assert.assertNotNull(schemas);
		Assert.assertEquals(1, schemas.size());

	}
}
