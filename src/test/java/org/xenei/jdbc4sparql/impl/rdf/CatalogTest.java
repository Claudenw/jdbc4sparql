package org.xenei.jdbc4sparql.impl.rdf;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

public class CatalogTest {
	private Model model;
	private Model dataModel;
	private RdfCatalog catalog;
	private EntityManager entityManager;

	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		dataModel = ModelFactory.createDefaultModel();
		catalog = new RdfCatalog.Builder().setName("testCatalog")
				.setLocalConnection(dataModel).build(model);
		entityManager = EntityManagerFactory.getEntityManager();
	}

	@After
	public void tearDown() throws Exception {
		catalog.close();
		model.close();
		dataModel.close();
	}

	@Test
	public void testClose() {
		Assert.assertFalse(model.isClosed());
		Assert.assertFalse(dataModel.isClosed());
		catalog.close();
		Assert.assertFalse(model.isClosed());
		Assert.assertFalse(dataModel.isClosed());
	}

	@Test
	public void testCloseMultiple() {
		new RdfCatalog.Builder().setName("testCatalog2")
				.setLocalConnection(dataModel).build(model);
		Assert.assertFalse(model.isClosed());
		Assert.assertFalse(dataModel.isClosed());
		catalog.close();
		Assert.assertFalse(model.isClosed());
		Assert.assertFalse(dataModel.isClosed());
	}

	@Test
	public void testExecuteLocalQuery() throws Exception {
		final Resource r = dataModel.createResource("http://example.com/res");
		final Property p = dataModel.createProperty("http://example.com/prop");
		r.addLiteral(p, "foo");
		r.addLiteral(p, "bar");

		final String qry = "Select * WHERE { ?s ?p ?o }";
		final Query query = QueryFactory.create(qry);

		List<QuerySolution> lqs = catalog.executeLocalQuery(query);
		Assert.assertEquals(2, lqs.size());

		/*
		 * // test reading from model final RdfCatalog cat2 =
		 * entityManager.read(catalog.getResource(), RdfCatalog.class); lqs =
		 * cat2.executeLocalQuery(query); Assert.assertEquals(2, lqs.size());
		 */
		// build a catalog with service node
		final RdfCatalog cat3 = new RdfCatalog.Builder()
				.setName("testCatalog2")
				.setSparqlEndpoint(new URL("http://example.com"))
				.setLocalConnection(dataModel).build(model);

		lqs = cat3.executeLocalQuery(query);
		Assert.assertEquals(2, lqs.size());

		/*
		 * // check reading from model final RdfCatalog cat4 =
		 * entityManager.read(cat3.getResource(), RdfCatalog.class);
		 * Assert.assertEquals("http://example.com", cat3.getServiceNode()
		 * .getURI()); lqs = cat4.executeLocalQuery(query);
		 * Assert.assertEquals(2, lqs.size());
		 */
	}

	@Test
	public void testExecuteQueryQuery() throws Exception {
		final Resource r = dataModel.createResource("http://example.com/res");
		final Property p = dataModel.createProperty("http://example.com/prop");
		r.addLiteral(p, "foo");
		r.addLiteral(p, "bar");

		final String qry = "Select * WHERE { ?s ?p ?o }";
		final Query query = QueryFactory.create(qry);

		final List<QuerySolution> lqs = catalog.executeQuery(query);
		Assert.assertEquals(2, lqs.size());

		new RdfCatalog.Builder().setName("testCatalog2")
				.setSparqlEndpoint(new URL("http://example.com")).build(model);
	}

	@Test
	public void testExecuteQueryString() throws Exception {

		final Resource r = dataModel.createResource("http://example.com/res");
		final Property p = dataModel.createProperty("http://example.com/prop");
		r.addLiteral(p, "foo");
		r.addLiteral(p, "bar");

		final String query = "Select * WHERE { ?s ?p ?o }";

		final List<QuerySolution> lqs = catalog.executeQuery(query);
		Assert.assertEquals(2, lqs.size());

		new RdfCatalog.Builder().setName("testCatalog2")
				.setSparqlEndpoint(new URL("http://example.com")).build(model);
	}

	@Test
	public void testFindSchemas() throws Exception {

		new RdfSchema.Builder().setName("testSchema1").setCatalog(catalog)
				.build(model);

		new RdfSchema.Builder().setName("testSchema2").setCatalog(catalog)
				.build(model);

		NameFilter<Schema> schemas = catalog.findSchemas(null);
		Assert.assertEquals(2, schemas.toList().size());

		schemas = catalog.findSchemas("");
		Assert.assertEquals(0, schemas.toList().size());

		schemas = catalog.findSchemas("testSchema1");
		Assert.assertEquals(1, schemas.toList().size());

		schemas = catalog.findSchemas("testSchema2");
		Assert.assertEquals(1, schemas.toList().size());

		entityManager.read(catalog.getResource(), RdfCatalog.class);

		schemas = catalog.findSchemas(null);
		Assert.assertEquals(2, schemas.toList().size());

		schemas = catalog.findSchemas("");
		Assert.assertEquals(0, schemas.toList().size());

		schemas = catalog.findSchemas("testSchema1");
		Assert.assertEquals(1, schemas.toList().size());

		schemas = catalog.findSchemas("testSchema2");
		Assert.assertEquals(1, schemas.toList().size());
	}

	@Test
	public void testGetName() {
		Assert.assertEquals("testCatalog", catalog.getName().getShortName());
	}

	@Test
	public void testGetResource() {
		Assert.assertNotNull(catalog.getResource());
		final Resource r = catalog.getResource();
		Assert.assertTrue(model.contains(r, null, (RDFNode) null));
	}

	@Test
	public void testGetSchema() throws Exception {
		Assert.assertNull(catalog.getSchema(null));
		Assert.assertNull(catalog.getSchema(""));

		Assert.assertNull(catalog.getSchema("testSchema1"));
		Assert.assertNull(catalog.getSchema("testSchema2"));

		new RdfSchema.Builder().setName("testSchema1").setCatalog(catalog)
				.build(model);

		Assert.assertNull(catalog.getSchema(null));
		Assert.assertNull(catalog.getSchema(""));

		Schema schema = catalog.getSchema("testSchema1");
		Assert.assertNotNull(schema);
		Assert.assertEquals("testSchema1", schema.getName().getShortName());

		Assert.assertNull(catalog.getSchema("testSchema2"));

		new RdfSchema.Builder().setName("testSchema2").setCatalog(catalog)
				.build(model);

		Assert.assertNull(catalog.getSchema(null));
		Assert.assertNull(catalog.getSchema(""));

		schema = catalog.getSchema("testSchema1");
		Assert.assertNotNull(schema);
		Assert.assertEquals("testSchema1", schema.getName().getShortName());

		schema = catalog.getSchema("testSchema2");
		Assert.assertNotNull(schema);
		Assert.assertEquals("testSchema2", schema.getName().getShortName());

		entityManager.read(catalog.getResource(), RdfCatalog.class);

		Assert.assertNull(catalog.getSchema(null));
		Assert.assertNull(catalog.getSchema(""));

		schema = catalog.getSchema("testSchema1");
		Assert.assertNotNull(schema);
		Assert.assertEquals("testSchema1", schema.getName().getShortName());

		schema = catalog.getSchema("testSchema2");
		Assert.assertNotNull(schema);
		Assert.assertEquals("testSchema2", schema.getName().getShortName());
	}

	@Test
	public void testGetSchemas() throws Exception {

		final List<String> names = new ArrayList<String>();

		Set<Schema> schemas = catalog.getSchemas();
		Assert.assertEquals(0, schemas.size());

		new RdfSchema.Builder().setName("testSchema1").setCatalog(catalog)
				.build(model);
		names.add("testSchema1");
		schemas = catalog.getSchemas();
		Assert.assertEquals(1, schemas.size());
		for (final Schema schema : schemas) {
			Assert.assertTrue(names.contains(schema.getName().getShortName()));
			Assert.assertEquals(catalog, schema.getCatalog());
		}

		new RdfSchema.Builder().setName("testSchema2").setCatalog(catalog)
				.build(model);
		names.add("testSchema2");
		schemas = catalog.getSchemas();
		Assert.assertEquals(2, schemas.size());
		for (final Schema schema : schemas) {
			Assert.assertTrue(names.contains(schema.getName().getShortName()));
		}

		// test reading from model
		final RdfCatalog cat2 = entityManager.read(catalog.getResource(),
				RdfCatalog.class);

		schemas = cat2.getSchemas();
		Assert.assertEquals(2, schemas.size());
		for (final Schema schema : schemas) {
			Assert.assertTrue(names.contains(schema.getName().getShortName()));
		}
	}

	@Test
	public void testGetServiceNode() throws Exception {
		Assert.assertNull(catalog.getServiceNode());
		// check reading from model
		final RdfCatalog cat2 = entityManager.read(catalog.getResource(),
				RdfCatalog.class);
		Assert.assertNull(cat2.getServiceNode());

		// build a catalog with service node
		final RdfCatalog cat3 = new RdfCatalog.Builder()
				.setName("testCatalog2")
				.setSparqlEndpoint(new URL("http://example.com")).build(model);

		Assert.assertEquals("http://example.com", cat3.getServiceNode()
				.getURI());

		entityManager.read(cat3.getResource(), RdfCatalog.class);
		Assert.assertEquals("http://example.com", cat3.getServiceNode()
				.getURI());
	}

	// @Test
	// public void testGetViewSchema() throws Exception
	// {
	// Schema schema = catalog.getViewSchema();
	// Assert.assertNotNull(schema);
	// Assert.assertEquals("", schema.getName().getShortName());
	//
	// // check reading from model
	// final RdfCatalog cat2 = entityManager.read(catalog.getResource(),
	// RdfCatalog.class);
	// schema = cat2.getViewSchema();
	// Assert.assertNotNull(schema);
	// Assert.assertEquals("", schema.getName().getShortName());
	// }

	@Test
	public void testIsService() throws Exception {
		Assert.assertFalse(catalog.isService());
		// check reading from model
		final RdfCatalog cat2 = entityManager.read(catalog.getResource(),
				RdfCatalog.class);
		Assert.assertFalse(cat2.isService());

		// build a catalog with service node
		final RdfCatalog cat3 = new RdfCatalog.Builder()
				.setName("testCatalog2")
				.setSparqlEndpoint(new URL("http://example.com")).build(model);

		Assert.assertTrue(cat3.isService());

		// check reading from model
		final RdfCatalog cat4 = entityManager.read(cat3.getResource(),
				RdfCatalog.class);
		Assert.assertTrue(cat4.isService());
	}

}
