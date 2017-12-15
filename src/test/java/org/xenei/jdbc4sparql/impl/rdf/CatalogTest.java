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
import org.xenei.jdbc4sparql.iface.QExecutor;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;

public class CatalogTest {
	private Model model;
	private Dataset dataset;
	private RDFConnection dataConnection;

	private RdfCatalog catalog;
	private EntityManager entityManager;

	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		entityManager = EntityManagerFactory.create( model );
		dataset = DatasetFactory.create();
			
		dataConnection = RDFConnectionFactory.connect( dataset );
		catalog = new RdfCatalog.Builder().setName("testCatalog")
				.setLocalConnection(dataConnection).build( entityManager );
	}

	@After
	public void tearDown() throws Exception {
		catalog.close();
		model.close();
		dataConnection.close();
	}

	@Test
	public void testClose() {
		Assert.assertFalse(model.isClosed());
		catalog.close();
		Assert.assertFalse(model.isClosed());
	}

	@Test
	public void testCloseMultiple() {
		new RdfCatalog.Builder().setName("testCatalog2")
				.setLocalConnection(dataConnection).build( entityManager );
		Assert.assertFalse(model.isClosed());
		Assert.assertFalse(dataConnection.isClosed());
		catalog.close();
		Assert.assertFalse(model.isClosed());
		Assert.assertTrue(dataConnection.isClosed());
		catalog.close();
		Assert.assertFalse(model.isClosed());
		Assert.assertTrue(dataConnection.isClosed());
		catalog.close();
	}

	@Test
	public void testExecuteLocalQuery() throws Exception {
		
		Model dataModel = ModelFactory.createDefaultModel();
		final Resource r = dataModel.createResource("http://example.com/res");
		final Property p = dataModel.createProperty("http://example.com/prop");
		r.addLiteral(p, "foo");
		r.addLiteral(p, "bar");
		dataset.setDefaultModel(dataModel);

		final String qry = "Select * WHERE { ?s ?p ?o }";
		final Query query = QueryFactory.create(qry);
		QExecutor qExec = catalog.getLocalExecutor();
		List<QuerySolution> lqs = QExecutor.asList( qExec, qExec.execute( query));
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
				.setLocalConnection(dataConnection).build( entityManager );
		QExecutor qExec3 = cat3.getLocalExecutor();
        
		lqs = QExecutor.asList(qExec3, qExec3.execute(query));
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
		Model dataModel = ModelFactory.createDefaultModel();
		final Resource r = dataModel.createResource("http://example.com/res");
		final Property p = dataModel.createProperty("http://example.com/prop");
		r.addLiteral(p, "foo");
		r.addLiteral(p, "bar");
		dataset.setDefaultModel(dataModel);
		
		final String qry = "Select * WHERE { ?s ?p ?o }";
		final Query query = QueryFactory.create(qry);
		QExecutor qExec = catalog.getLocalExecutor();
        final List<QuerySolution> lqs = QExecutor.asList(qExec, qExec.execute(query));
		Assert.assertEquals(2, lqs.size());

		new RdfCatalog.Builder().setName("testCatalog2")
				.setSparqlEndpoint(new URL("http://example.com")).build( entityManager );
	}

	@Test
	public void testExecuteQueryString() throws Exception {
		Model dataModel = ModelFactory.createDefaultModel();
		final Resource r = dataModel.createResource("http://example.com/res");
		final Property p = dataModel.createProperty("http://example.com/prop");
		r.addLiteral(p, "foo");
		r.addLiteral(p, "bar");
		dataset.setDefaultModel(dataModel);

		final String query = "Select * WHERE { ?s ?p ?o }";
		QExecutor qExec = catalog.getLocalExecutor();
		final List<QuerySolution> lqs = QExecutor.asList( qExec, QExecutor.execute( qExec,query));
		Assert.assertEquals(2, lqs.size());

		new RdfCatalog.Builder().setName("testCatalog2")
				.setSparqlEndpoint(new URL("http://example.com")).build( entityManager );
	}

	@Test
	public void testFindSchemas() throws Exception {

		new RdfSchema.Builder().setName("testSchema1").setCatalog(catalog)
				.build();

		new RdfSchema.Builder().setName("testSchema2").setCatalog(catalog)
				.build();

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
				.build();

		Assert.assertNull(catalog.getSchema(null));
		Assert.assertNull(catalog.getSchema(""));

		Schema schema = catalog.getSchema("testSchema1");
		Assert.assertNotNull(schema);
		Assert.assertEquals("testSchema1", schema.getName().getShortName());

		Assert.assertNull(catalog.getSchema("testSchema2"));

		new RdfSchema.Builder().setName("testSchema2").setCatalog(catalog)
				.build(  );

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
				.build(  );
		names.add("testSchema1");
		schemas = catalog.getSchemas();
		Assert.assertEquals(1, schemas.size());
		for (final Schema schema : schemas) {
			Assert.assertTrue(names.contains(schema.getName().getShortName()));
			Assert.assertEquals(catalog, schema.getCatalog());
		}

		new RdfSchema.Builder().setName("testSchema2").setCatalog(catalog)
				.build( );
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



}
