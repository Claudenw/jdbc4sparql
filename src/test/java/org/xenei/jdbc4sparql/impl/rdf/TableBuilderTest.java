package org.xenei.jdbc4sparql.impl.rdf;

import java.util.List;

import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.name.SchemaName;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;

public class TableBuilderTest {

	private Model model;
	private RdfTableDef tableDef;
	private RdfSchema mockSchema;
	private EntityManager mgr;

	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		mgr = EntityManagerFactory.create( model );
		final RdfTableDef.Builder builder = new RdfTableDef.Builder()
				.addColumnDef(
						RdfColumnDef.Builder.getStringBuilder().build( mgr ))
				.addColumnDef(
						RdfColumnDef.Builder.getIntegerBuilder().build( mgr ));
		tableDef = builder.build( mgr );
		mockSchema = mock(RdfSchema.class);
		when(mockSchema.getResource()).thenReturn(
				model.createResource("http://example.com/mockSchema"));
		when(mockSchema.getName()).thenReturn(
				new SchemaName("catalog", "schema"));
		when(mockSchema.getEntityManager()).thenReturn(mgr);
		
	}

	@After
	public void tearDown() throws Exception {
		model.close();
	}

	@Test
	public void testDefaultBuilder() throws Exception {
		final RdfTable.Builder builder = new RdfTable.Builder()
				.setTableDef(tableDef).setName("table")
				.setColumn(0, "StringCol").setColumn(1, "IntCol")
				.setSchema(mockSchema).setType("testing Table");
		final RdfTable table = builder.build(  );

		Assert.assertEquals(2, table.getColumnCount());
		Assert.assertEquals("table", table.getName().getShortName());
		final NameFilter<Column> nf = table.findColumns("StringCol");
		Assert.assertTrue(nf.hasNext());
		final Column c = nf.next();
		Assert.assertEquals("StringCol", c.getName().getShortName());
		Assert.assertFalse(nf.hasNext());

		EntityManagerFactory.create();

		final Property p = model.createProperty(
				ResourceBuilder.getNamespace(mgr,RdfTable.class), "column");

		final List<RDFNode> columns = table.getResource()
				.getRequiredProperty(p).getResource().as(RDFList.class)
				.asJavaList();

		Assert.assertEquals(2, columns.size());

	}

}
