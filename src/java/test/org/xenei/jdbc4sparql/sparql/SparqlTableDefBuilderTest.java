package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.sql.Types;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SparqlTableDefBuilderTest
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
		final SparqlTableDef.Builder builder = new SparqlTableDef.Builder();
		final SparqlColumnDef.Builder colBuilder = new SparqlColumnDef.Builder();

		colBuilder.setType(Types.VARCHAR).setSigned(false);
		colBuilder.addQuerySegment("Segment1");
		final SparqlColumnDef columnDef1 = colBuilder.build(model);
		builder.addColumnDef(columnDef1);

		colBuilder.setType(Types.INTEGER).setSigned(true);
		colBuilder.addQuerySegment("Segment2");
		final SparqlColumnDef columnDef2 = colBuilder.build(model);
		builder.addColumnDef(columnDef2);

		builder.addQuerySegment("Segment1A").addQuerySegment("Segment2A");

		final SparqlTableDef tableDef = builder.build(model);

		Assert.assertEquals(2, tableDef.getColumnCount());

		Assert.assertEquals(ColumnDefBuilder.getStringBuilder().build(model),
				tableDef.getColumnDef(0));
		Assert.assertEquals(columnDef1, tableDef.getColumnDef(0));
		Assert.assertTrue(tableDef.getColumnDef(0) instanceof SparqlColumnDef);

		Assert.assertEquals(ColumnDefBuilder.getIntegerBuilder().build(model),
				tableDef.getColumnDef(1));
		Assert.assertEquals(columnDef2, tableDef.getColumnDef(1));
		Assert.assertTrue(tableDef.getColumnDef(1) instanceof SparqlColumnDef);

		Assert.assertEquals(1, tableDef.getColumnIndex(ColumnDefBuilder
				.getIntegerBuilder().build(model)));
		Assert.assertNotNull(tableDef.getColumnDefs());
		Assert.assertNull(tableDef.getPrimaryKey());
		Assert.assertNull(tableDef.getSortKey());
		Assert.assertNull(tableDef.getSuperTableDef());

		final List<String> lst = tableDef.getQuerySegments();
		Assert.assertNotNull(lst);
		Assert.assertEquals(2, lst.size());
		Assert.assertEquals("Segment1A", lst.get(0));
		Assert.assertEquals("Segment2A", lst.get(1));

	}

}
