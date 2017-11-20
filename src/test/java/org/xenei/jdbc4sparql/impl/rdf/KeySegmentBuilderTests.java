package org.xenei.jdbc4sparql.impl.rdf;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.KeySegment;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class KeySegmentBuilderTests {
	private Model model;

	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
	}

	@After
	public void tearDown() throws Exception {
		model.close();
	}

	@Test
	public void testDefault() {
		final RdfKeySegment.Builder builder = new RdfKeySegment.Builder();
		final KeySegment seg = builder.build(model);

		Assert.assertEquals(0, seg.getIdx());
		Assert.assertEquals(true, seg.isAscending());

	}

	@Test
	public void testInvalid() {
		final RdfKeySegment.Builder builder = new RdfKeySegment.Builder();
		try {
			builder.setIdx(-1);
			Assert.fail("should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			// expected
		}

		try {
			builder.setIdx(Short.MAX_VALUE + 1);

			Assert.fail("should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			// expected
		}
	}

	@Test
	public void testSetValues() {
		final RdfKeySegment.Builder builder = new RdfKeySegment.Builder()
				.setIdx(5).setAscending(false);
		final KeySegment seg = builder.build(model);

		Assert.assertEquals(5, seg.getIdx());
		Assert.assertEquals(false, seg.isAscending());

	}

}
