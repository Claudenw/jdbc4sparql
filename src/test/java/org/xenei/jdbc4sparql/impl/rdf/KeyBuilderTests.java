package org.xenei.jdbc4sparql.impl.rdf;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class KeyBuilderTests {
	private Model model;
	private EntityManager mgr;
	
	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		mgr = new EntityManagerImpl( model );
	}

	@After
	public void tearDown() throws Exception {
		model.close();
	}

	@Test
	public void testDefault() {
		final RdfKeySegment.Builder segBuilder = new RdfKeySegment.Builder();

		final RdfKey.Builder builder = new RdfKey.Builder()
				.addSegment(segBuilder.build( mgr ));

		final Key key = builder.build( mgr );

		Assert.assertEquals(false, key.isUnique());
		Assert.assertEquals(1, key.getSegments().size());
		Assert.assertEquals(segBuilder.build( mgr ), key.getSegments().get(0));

	}

	@Test
	public void testMultipleSegments() {
		final RdfKeySegment.Builder segBuilder = new RdfKeySegment.Builder();

		final RdfKey.Builder builder = new RdfKey.Builder().addSegment(
				segBuilder.build( mgr )).addSegment(
				segBuilder.setAscending(false).setIdx(1).build( mgr ));

		final Key key = builder.build( mgr );

		Assert.assertEquals(false, key.isUnique());
		Assert.assertEquals(2, key.getSegments().size());
		Assert.assertEquals(segBuilder.build( mgr ), key.getSegments().get(1));
	}

	@Test
	public void testUnique() {
		final RdfKeySegment.Builder segBuilder = new RdfKeySegment.Builder();

		final RdfKey.Builder builder = new RdfKey.Builder().addSegment(
				segBuilder.build( mgr )).setUnique(true);

		final Key key = builder.build( mgr );

		Assert.assertEquals(true, key.isUnique());
		Assert.assertEquals(1, key.getSegments().size());
		Assert.assertEquals(segBuilder.build( mgr ), key.getSegments().get(0));
	}
}
