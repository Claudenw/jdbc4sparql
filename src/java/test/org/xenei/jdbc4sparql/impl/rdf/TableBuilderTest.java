package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;

public class TableBuilderTest
{

	private Model model;
	private RdfTableDef tableDef;
	private RdfSchema mockSchema;

	@Before
	public void setUp() throws Exception
	{
		model = ModelFactory.createDefaultModel();
		final RdfTableDef.Builder builder = new RdfTableDef.Builder().addColumnDef(
				RdfColumnDef.Builder.getStringBuilder().build(model)).addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build(model));
		tableDef = builder.build(model);
		mockSchema = Mockito.mock(RdfSchema.class);
		Mockito.when(mockSchema.getResource()).thenReturn(
				model.createResource("http://example.com/mockSchema"));

	}

	@After
	public void tearDown() throws Exception
	{
		model.close();
	}

	@Test
	public void testDefaultBuilder() throws Exception
	{
		final RdfTable.Builder builder = new RdfTable.Builder().setTableDef(tableDef)
				.setName("table").setColumn(0, "StringCol")
				.setColumn(1, "IntCol").setSchema(mockSchema)
				.setType("testing Table");
		final RdfTable table = builder.build(model);

		model.write(System.out, "TURTLE");

		Assert.assertEquals(2, table.getColumnCount());
		Assert.assertEquals("table", table.getName());
		final NameFilter<Column> nf = table.findColumns("StringCol");
		Assert.assertTrue(nf.hasNext());
		final Column c = nf.next();
		Assert.assertEquals("StringCol", c.getName());
		Assert.assertFalse(nf.hasNext());

		// check the columns
		EntityManager entityManager = EntityManagerFactory.getEntityManager();
		final Property p = entityManager.getSubjectInfo(RdfColumn.class)
				.getPredicateProperty("getTable");
		List<RdfColumn> columns = new ArrayList<RdfColumn>();
		final Model model = table.getResource().getModel();

		List<Resource> lr = model.listSubjectsWithProperty(p,
					table.getResource()).toList();
		
		Assert.assertEquals( 2, lr.size() );
		for (final Resource r : lr )
		{
			columns.add(entityManager.read(r, RdfColumn.class));
		}
		Assert.assertEquals( 2, columns.size() );
		
	}

}
