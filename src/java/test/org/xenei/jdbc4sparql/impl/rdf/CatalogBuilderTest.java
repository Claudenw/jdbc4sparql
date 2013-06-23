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
	private RdfSchema.Builder schemaBldr;

	@Before
	public void setUp() throws Exception
	{
		model = ModelFactory.createDefaultModel();

		schemaBldr = new RdfSchema.Builder().setName("testSchema");
	}

	@After
	public void tearDown() throws Exception
	{
		model.close();
	}

	@Test
	public void testAddSchema()
	{
		final RdfCatalog.Builder builder = new RdfCatalog.Builder().setName("catalog");

		final RdfCatalog catalog = builder.build(model);
		Assert.assertEquals("catalog", catalog.getName());
		Assert.assertNotNull(catalog.getSchemas());
		Assert.assertEquals(1, catalog.getSchemas().size());
		schemaBldr.setCatalog(catalog);

		final Schema schema = schemaBldr.build(model);

		Assert.assertNotNull(catalog.getSchemas());
		Assert.assertEquals(2, catalog.getSchemas().size());
	}

	@Test
	public void testDefault()
	{
		final RdfCatalog.Builder builder = new RdfCatalog.Builder().setName("catalog");

		final Catalog catalog = builder.build(model);

		Assert.assertEquals("catalog", catalog.getName());
		Assert.assertNotNull(catalog.getSchemas());
		Assert.assertEquals(1, catalog.getSchemas().size());
		Assert.assertNotNull( catalog.getSchema( Catalog.DEFAULT_SCHEMA));
	}

	@Test
	public void testFindSchema()
	{
		final RdfCatalog.Builder builder = new RdfCatalog.Builder().setName("catalog");

		final RdfCatalog catalog = builder.build(model);

		schemaBldr.setCatalog(catalog).build(model);

		Assert.assertNotNull(catalog.findSchemas("testSchema"));

		Assert.assertNotNull(catalog.findSchemas(null));
	}

	@Test
	public void testGetSchema() throws Exception
	{
		final RdfCatalog.Builder builder = new RdfCatalog.Builder().setName("catalog");

		final RdfCatalog catalog = builder.build(model);

		schemaBldr.setCatalog(catalog).build(model);

		model.write(System.out, "TURTLE");
		model.write(new FileOutputStream(new File("dump.ttl")), "TURTLE");

		Assert.assertNotNull(catalog.getSchema("testSchema"));

	}

	@Test
	public void testGetSchemas()
	{
		final RdfCatalog.Builder builder = new RdfCatalog.Builder().setName("catalog");

		final RdfCatalog catalog = builder.build(model);

		// should contain default schema
		Set<? extends Schema> schemas = catalog.getSchemas();
		Assert.assertNotNull(schemas);
		Assert.assertEquals(1, schemas.size());
		Assert.assertNotNull(catalog.getSchema( Catalog.DEFAULT_SCHEMA ));
		
		schemaBldr.setCatalog(catalog);

		final RdfSchema schema = schemaBldr.build(model);

		schemas = catalog.getSchemas();
		Assert.assertNotNull(schemas);
		Assert.assertEquals(2, schemas.size());

	}
}
