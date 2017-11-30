package org.xenei.jdbc4sparql.sparql.items;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLDataException;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.NameSegments;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.impl.NameUtils;

import org.apache.jena.sparql.core.Var;

public class QueryColumnInfoTest {

	private QueryColumnInfo columnInfo;
	private Column column;
	private ColumnName columnName;
	private ColumnDef colDef;

	@Before
	public void setup() {
		columnName = new ColumnName("catalog", "schema", "table", "column");

		colDef = mock(ColumnDef.class);
		when(colDef.getType()).thenReturn(java.sql.Types.VARCHAR);

		column = mock(Column.class);
		when(column.getName()).thenReturn(columnName);
		when(column.getColumnDef()).thenReturn(colDef);

		columnInfo = new QueryColumnInfo(column, false);
	}

	@Test
	public void testSegments() {
		assertEquals("FTTT", columnInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.CATALOG);
		assertEquals("TFFT", columnInfo.getSegments()
				.toString());
		assertEquals("catalog.schema.table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.SCHEMA);
		assertEquals("FTTT", columnInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.TABLE);
		assertEquals("FTTT", columnInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.FFTF);
		assertEquals("FFTT", columnInfo.getSegments()
				.toString());
		assertEquals("table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.TTTF);
		assertEquals("TTTT", columnInfo.getSegments()
				.toString());
		assertEquals("catalog.schema.table.column", columnInfo.getName().getDBName());

	}

	@Test
	public void testGetName() {
		final ItemName name = columnInfo.getName();
		assertTrue(name == columnName);
	}

	@Test
	public void testGetVar() {
		assertEquals(GUIDObject.asVar( columnName), columnInfo.getGUIDVar());
	}

	@Test
	public void testGetGUID() {
		assertEquals( columnInfo.getGUID(), columnInfo.getName().getGUID());
	}

	@Test
	public void testIsOptional() {
		assertFalse(columnInfo.isOptional());
		columnInfo.setOptional(true);
		assertTrue(columnInfo.isOptional());
	}

	@Test
	public void testAddAlias() throws SQLDataException {
		final ColumnName alias = new ColumnName("", "", "", "alias");
		final QueryColumnInfo aliasInfo = columnInfo.createAlias(alias);
		assertEquals(alias, aliasInfo.getName());

		assertNotEquals(columnInfo.getGUID(), alias.getGUID());
		assertEquals(columnInfo.getGUID(), aliasInfo.getGUID());

	}

	@Test
	public void testGetColumn() {
		assertEquals(column, columnInfo.getColumn());
	}

}
