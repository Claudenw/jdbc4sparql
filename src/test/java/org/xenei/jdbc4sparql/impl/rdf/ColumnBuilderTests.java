package org.xenei.jdbc4sparql.impl.rdf;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.name.TableName;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class ColumnBuilderTests {
	private Model model;
	private ColumnDef columnDef;
	private RdfTable mockTable;

	@Before
	public void setUp() throws Exception {
		model = ModelFactory.createDefaultModel();
		columnDef = RdfColumnDef.Builder.getStringBuilder().build(model);
		mockTable = mock(RdfTable.class);
		when(mockTable.getResource()).thenReturn(
				model.createResource("http://example.com/mockTable"));
		when(mockTable.getName()).thenReturn(
				new TableName("catalog", "schema", "table"));
	}

	@After
	public void tearDown() throws Exception {
		model.close();
	}

	@Test
	public void testStandardCreation() {
		final RdfColumn.Builder builder = new RdfColumn.Builder()
				.setColumnDef(columnDef).setName("test").setTable(mockTable);
		final Column cd = builder.build(model);
		Assert.assertEquals("test", cd.getName().getShortName());
		Assert.assertEquals(false, cd.getColumnDef().isAutoIncrement());
		Assert.assertEquals(false, cd.getColumnDef().isCaseSensitive());
		Assert.assertEquals("", cd.getColumnDef().getColumnClassName());
		Assert.assertEquals(false, cd.getColumnDef().isCurrency());
		Assert.assertEquals(false, cd.getColumnDef().isDefinitelyWritable());
		Assert.assertEquals(0, cd.getColumnDef().getDisplaySize());
		Assert.assertEquals(0, cd.getColumnDef().getNullable());
		Assert.assertEquals(0, cd.getColumnDef().getPrecision());
		Assert.assertEquals(false, cd.getColumnDef().isReadOnly());
		Assert.assertEquals(0, cd.getColumnDef().getScale());
		Assert.assertEquals(false, cd.getColumnDef().isSearchable());
		Assert.assertEquals(false, cd.getColumnDef().isSigned());
		Assert.assertEquals("String", cd.getColumnDef().getTypeName());
		Assert.assertEquals(false, cd.getColumnDef().isWritable());
	}
}
