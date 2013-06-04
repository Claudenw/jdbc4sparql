package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SparqlColumnDefBuilderTest
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
	public void testCreation()
	{
		final SparqlColumnDef.Builder builder = new SparqlColumnDef.Builder();

		builder.setType(Types.VARCHAR).setAutoIncrement(true)
				.setCaseSensitive(true).setColumnClassName("foo")
				.setCurrency(true).setDefinitelyWritable(true)
				.setDisplaySize(5)
				.setNullable(DatabaseMetaData.columnNullableUnknown)
				.setPrecision(3).setReadOnly(true).setScale(10)
				.setSearchable(true).setSigned(true).setTypeName("bar")
				.setWritable(true);
		builder.addQuerySegment("Segment1").addQuerySegment("Segment2");

		final SparqlColumnDef cd = builder.build(model);
		model.write(System.out, "TURTLE");

		Assert.assertEquals(true, cd.isAutoIncrement());
		Assert.assertEquals(true, cd.isCaseSensitive());
		Assert.assertEquals("foo", cd.getColumnClassName());
		Assert.assertEquals(true, cd.isCurrency());
		Assert.assertEquals(true, cd.isDefinitelyWritable());
		Assert.assertEquals(5, cd.getDisplaySize());
		Assert.assertEquals(DatabaseMetaData.columnNullableUnknown,
				cd.getNullable());
		Assert.assertEquals(3, cd.getPrecision());
		Assert.assertEquals(true, cd.isReadOnly());
		Assert.assertEquals(10, cd.getScale());
		Assert.assertEquals(true, cd.isSearchable());
		Assert.assertEquals(true, cd.isSigned());
		Assert.assertEquals("bar", cd.getTypeName());
		Assert.assertEquals(true, cd.isWritable());

		final List<String> lst = cd.getQuerySegments();
		Assert.assertNotNull(lst);
		Assert.assertEquals(2, lst.size());
		Assert.assertEquals("Segment1", lst.get(0));
		Assert.assertEquals("Segment2", lst.get(1));

	}

}
