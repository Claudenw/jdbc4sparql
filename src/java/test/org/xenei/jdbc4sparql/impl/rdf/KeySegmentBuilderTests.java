package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.KeySegment;

public class KeySegmentBuilderTests
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
		final KeySegmentBuilder builder = new KeySegmentBuilder();
		final KeySegment seg = builder.build(model);

		Assert.assertEquals(0, seg.getIdx());
		Assert.assertEquals(true, seg.isAscending());

	}

	@Test
	public void testInvalid()
	{
		final KeySegmentBuilder builder = new KeySegmentBuilder();
		try
		{
			builder.setIdx(-1);
			Assert.fail("should have thrown IllegalArgumentException");
		}
		catch (final IllegalArgumentException expected)
		{
			// expected
		}

		try
		{
			builder.setIdx(Short.MAX_VALUE + 1);

			Assert.fail("should have thrown IllegalArgumentException");
		}
		catch (final IllegalArgumentException expected)
		{
			// expected
		}
	}

	@Test
	public void testSetValues()
	{
		final KeySegmentBuilder builder = new KeySegmentBuilder().setIdx(5)
				.setAscending(false);
		final KeySegment seg = builder.build(model);

		Assert.assertEquals(5, seg.getIdx());
		Assert.assertEquals(false, seg.isAscending());

	}

}
