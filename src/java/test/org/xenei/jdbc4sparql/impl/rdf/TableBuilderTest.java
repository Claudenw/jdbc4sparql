package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

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

public class TableBuilderTest
{

	private Model model;
	private TableDef tableDef;
	private Schema mockSchema;

	@Before
	public void setUp() throws Exception
	{
		model = ModelFactory.createDefaultModel();
		final TableDefBuilder builder = new TableDefBuilder()
				.addColumnDef(ColumnDefBuilder.getStringBuilder().build(model))
				.addColumnDef(ColumnDefBuilder.getIntegerBuilder().build(model));
		tableDef = builder.build(model);
		mockSchema = Mockito.mock(Schema.class);
		Mockito.when( mockSchema.getResource()).thenReturn( model.createResource("http://example.com/mockSchema"));

	}

	@After
	public void tearDown() throws Exception
	{
		model.close();
	}

	@Test
	public void testDefaultBuilder()
	{
		TableBuilder builder = new TableBuilder()
			.setTableDef(tableDef)
			.setName( "table" )
			.setColumn(0, "StringCol" )
			.setColumn(1, "IntCol" )
			.setSchema( mockSchema )
			.setType( "testing Table");
		Table table = builder.build( model );			
		
		model.write( System.out, "TURTLE");
		
		Assert.assertEquals(2, table.getColumnCount());
		Assert.assertEquals( "table", table.getName() );
		NameFilter<Column> nf = table.findColumns( "StringCol");
		Assert.assertTrue( nf.hasNext() );
		Column c = nf.next();
		Assert.assertEquals( "StringCol", c.getName());
		Assert.assertFalse(nf.hasNext());
		
	}

}
