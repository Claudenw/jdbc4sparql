package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Key;

public class KeyBuilderTests
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
	public void testDefault()
	{
		final RdfKeySegment.Builder segBuilder = new RdfKeySegment.Builder();

		final RdfKey.Builder builder = new RdfKey.Builder()
		.addSegment(segBuilder.build(model));

		final Key key = builder.build(model);

		Assert.assertEquals(false, key.isUnique());
		Assert.assertEquals(1, key.getSegments().size());
		Assert.assertEquals(segBuilder.build(model), key.getSegments().get(0));

	}

	@Test
	public void testMultipleSegments()
	{
		final RdfKeySegment.Builder segBuilder = new RdfKeySegment.Builder();

		final RdfKey.Builder builder = new RdfKey.Builder().addSegment(
				segBuilder.build(model)).addSegment(
						segBuilder.setAscending(false).setIdx(1).build(model));

		final Key key = builder.build(model);

		Assert.assertEquals(false, key.isUnique());
		Assert.assertEquals(2, key.getSegments().size());
		Assert.assertEquals(segBuilder.build(model), key.getSegments().get(1));
	}

	@Test
	public void testUnique()
	{
		final RdfKeySegment.Builder segBuilder = new RdfKeySegment.Builder();

		final RdfKey.Builder builder = new RdfKey.Builder().addSegment(
				segBuilder.build(model)).setUnique(true);

		final Key key = builder.build(model);

		Assert.assertEquals(true, key.isUnique());
		Assert.assertEquals(1, key.getSegments().size());
		Assert.assertEquals(segBuilder.build(model), key.getSegments().get(0));
	}
}
