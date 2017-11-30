package org.xenei.jdbc4sparql.sparql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLDataException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.NameSegments;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.SchemaName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryItemCollection;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.utils.ElementExtractor;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;

public class QueryInfoSetTest {

	private QueryInfoSet queryInfo;

	private Column column;
	private ColumnName cName;

	private Table table;
	private TableName tName;

	private Column column2;
	private ColumnName cName2;

	private Table table2;
	private TableName tName2;

	private List<Column> cols2;

	@Before
	public void setup() {
		queryInfo = new QueryInfoSet();

		column = mock(Column.class);
		cName = new ColumnName("catalog", "schema", "table", "column");
		when(column.getName()).thenReturn(cName);
		when(column.getQuerySegmentFmt()).thenReturn(" { %s <b> %s } ");
		when(column.hasQuerySegments()).thenReturn(true);
		final ColumnDef colDef = mock(ColumnDef.class);
		when(column.getColumnDef()).thenReturn(colDef);
		when(colDef.getType()).thenReturn(java.sql.Types.VARCHAR);

		table = mock(Table.class);
		tName = new TableName("catalog", "schema", "table");
		when(table.getName()).thenReturn(tName);
		when(table.getQuerySegmentFmt()).thenReturn("{ ?tbl <a> %s }");
		when(table.getColumn(eq("column"))).thenReturn(column);

		final List<Column> cols = new ArrayList<Column>();
		cols.add(column);
		when(table.getColumns()).thenAnswer(new ColumnAnswer(cols));

		column2 = mock(Column.class);
		cName2 = new ColumnName("catalog", "schema2", "table", "column");
		when(column2.getName()).thenReturn(cName2);
		when(column2.getQuerySegmentFmt()).thenReturn(" { %s <b> %s } ");
		when(column2.hasQuerySegments()).thenReturn(true);

		table2 = mock(Table.class);
		tName2 = new TableName("catalog", "schema2", "table");
		when(table2.getName()).thenReturn(tName2);
		when(table2.getQuerySegmentFmt()).thenReturn("{ ?tbl <a> %s }");
		when(table2.getColumn(eq("column"))).thenReturn(column2);

		cols2 = new ArrayList<Column>();
		cols2.add(column2);
		when(table2.getColumns()).thenAnswer(new ColumnAnswer(cols2));

	}

	private static class ColumnAnswer implements Answer<Iterator<Column>> {
		List<Column> cols;

		ColumnAnswer(final List<Column> cols) {
			this.cols = cols;
		}

		@Override
		public Iterator<Column> answer(final InvocationOnMock invocation)
				throws Throwable {
			return cols.iterator();
		}
	}

	@Test
	public void testAddColumn() {
		final QueryColumnInfo columnInfo = new QueryColumnInfo(column);
		queryInfo.addColumn(columnInfo);
		final QueryColumnInfo found = queryInfo.findColumn(column);// queryInfo.findColumnByGUID(cName);
		assertEquals(found, columnInfo);
	}

	@Test
	public void testAddDefinedColumns() throws SQLDataException {
		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());

		final ElementExtractor extractor = new ElementExtractor(
				ElementPathBlock.class);
		extractor.visit(queryElementGroup);

		ElementPathBlock epb = (ElementPathBlock) extractor.getExtracted().get(
				0);
		TriplePath pth = epb.patternElts().next();
		assertEquals(Var.alloc("tbl"), pth.asTriple().getSubject());
		assertEquals(NodeFactory.createURI("a"), pth.asTriple().getPredicate());
		assertEquals(GUIDObject.asVar(tName), pth.asTriple().getObject());

		epb = (ElementPathBlock) extractor.getExtracted().get(1);
		pth = epb.patternElts().next();
		assertEquals(GUIDObject.asVar(tName), pth.asTriple().getSubject());
		assertEquals(NodeFactory.createURI("b"), pth.asTriple().getPredicate());
		assertEquals(GUIDObject.asVar(cName), pth.asTriple().getObject());

	}

	// @Test
	// private void testGetSchemasInQuery()
	// {
	// Column column2 = mock( Column.class );
	// ColumnName cName2 = new ColumnName( "catalog", "schema2", "table",
	// "column");
	// when( column2.getName()).thenReturn( cName2 );
	// when( column2.getQuerySegmentFmt() ).thenReturn( " { %s <b> %s } ");
	//
	// Table table2 = mock( Table.class );
	// TableName tName2 = new TableName( "catalog", "schema", "table" );
	// when( table2.getName() ).thenReturn( tName2 );
	// when( table2.getQuerySegmentFmt()).thenReturn( "{ ?tbl <a> %s }");
	//
	// List<Column> cols = new ArrayList<Column>();
	// cols.add(column2);
	// when( table2.getColumns()).thenReturn( cols.iterator() );
	// //queryInfo.addTable(table2 );
	//
	// //List<String> schemas = queryInfo.getSchemasInQuery();
	// }

	@Test
	public void testAddTable() {
		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);

		assertEquals(NameSegments.FTTF, tableInfo.getSegments());
		assertEquals(1, queryInfo.getTables().size());
		assertEquals(tableInfo.getGUID(), queryInfo.getTables().iterator()
				.next().getGUID());

		final QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup, table2, false);
		queryInfo.addTable(tableInfo2);

		assertEquals(NameSegments.FTTF, tableInfo.getSegments());
		assertEquals(NameSegments.FTTF, tableInfo2.getSegments());
		assertEquals(2, queryInfo.getTables().size());
		final Set<String> guids = new HashSet<String>();
		for (final QueryTableInfo qti : queryInfo.getTables()) {
			guids.add(qti.getGUID());
		}
		assertEquals(2, guids.size());

		assertTrue(guids.contains(tableInfo.getGUID()));
		assertTrue(guids.contains(tableInfo2.getGUID()));
	}

	@Test
	public void testContainsColumn() throws SQLDataException {
		assertFalse(queryInfo.containsColumn(cName2));
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertTrue(queryInfo.containsColumn(cName2));

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());
		;
		assertTrue(queryInfo.containsColumn(cName));

		assertTrue(queryInfo.containsColumn(new TableName("catalog", "schema2",
				"table")));
		assertFalse(queryInfo.containsColumn(new TableName("catalog",
				"schema2", "table2")));
		assertTrue(queryInfo
				.containsColumn(new SchemaName("catalog", "schema2")));
		assertFalse(queryInfo.containsColumn(new SchemaName("catalog",
				"schema3")));
		assertTrue(queryInfo.containsColumn(new CatalogName("catalog")));
		assertFalse(queryInfo.containsColumn(new CatalogName("catalog2")));

	}

	@Test
	public void testFindColumn() throws SQLDataException {
		assertNull(queryInfo.findColumn(cName2));
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(cName2, queryInfo.findColumn(cName2).getName());
		assertEquals(cName2, queryInfo.findColumn(new ColumnName(cName2))
				.getName());

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());
		assertEquals(cName, queryInfo.findColumn(cName).getName());

		assertNull(queryInfo.findColumn(new ColumnName("catalog", "schema2",
				"table", "column2")));

	}

	// @Test
	// public void testFindColumnByGUID() {
	// assertNull(queryInfo.findColumnByGUID(cName2));
	// queryInfo.addColumn(new QueryColumnInfo(column2));
	// assertEquals(cName2, queryInfo.findColumnByGUID(cName2).getName());
	// assertEquals(cName2, queryInfo.findColumnByGUID(new ColumnName(cName2))
	// .getName());
	//
	// ElementGroup queryElementGroup = new ElementGroup();
	// QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
	// queryElementGroup, table, false);
	// queryInfo.addTable(tableInfo);
	// queryInfo.addRequiredColumns();
	// assertEquals(cName, queryInfo.findColumnByGUID(cName).getName());
	//
	// assertNull(queryInfo.findColumnByGUID(new ColumnName("catalog",
	// "schema2", "table", "column2")));
	//
	// }

	@Test
	public void testGetColumnByName_ColumnName() throws SQLDataException {
		try {
			queryInfo.getColumn(cName2);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {

		}
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(cName2, queryInfo.getColumn(cName2).getName());
		assertEquals(cName2, queryInfo.getColumn(new ColumnName(cName2))
				.getName());

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());
		assertEquals(cName, queryInfo.getColumn(cName).getName());

		try {
			queryInfo.getColumn(new ColumnName("catalog", "schema2", "table",
					"column2"));
		} catch (final IllegalArgumentException expected) {

		}
	}

	@Test
	public void testGetColumnIndex() throws SQLDataException {

		assertEquals(-1, queryInfo.getColumnIndex(cName2));

		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(0, queryInfo.getColumnIndex(cName2));
		assertEquals(0, queryInfo.getColumnIndex(new ColumnName(cName2)));

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());
		assertEquals(1, queryInfo.getColumnIndex(cName));

		assertEquals(-1, queryInfo.getColumnIndex(new ColumnName("catalog",
				"schema2", "table", "column2")));
	}

	@Test
	public void testGetColumns() throws SQLDataException {
		assertEquals(0, queryInfo.getColumns().size());
		final QueryColumnInfo colInfo1 = new QueryColumnInfo(column2);
		queryInfo.addColumn(colInfo1);
		assertEquals(1, queryInfo.getColumns().size());

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());
		final QueryItemCollection<QueryColumnInfo, Column, ColumnName> cols = queryInfo
				.getColumns();
		assertEquals(2, cols.size());
		assertEquals(colInfo1, cols.get(0));
		assertEquals(queryInfo.getColumn(cName), cols.get(1));
	}

	@Test
	public void testGetTable() {
		assertNull(queryInfo.getTable(table.getName()));
		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);

		assertEquals(tableInfo, queryInfo.getTable(table.getName()));
		assertNull(queryInfo.getTable(table2.getName()));

		final ElementGroup queryElementGroup2 = new ElementGroup();
		final QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		queryInfo.addTable(tableInfo2);

		assertEquals(tableInfo, queryInfo.getTable(table.getName()));
		assertEquals(tableInfo2, queryInfo.getTable(table2.getName()));

		assertNull(queryInfo.getTable(new TableName("catalog", "schema",
				"table3")));

	}

	@Test
	public void testGetTables() {
		assertEquals(0, queryInfo.getTables().size());

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		assertEquals(0, queryInfo.getTables().size());
		queryInfo.addTable(tableInfo);
		assertEquals(1, queryInfo.getTables().size());

		final ElementGroup queryElementGroup2 = new ElementGroup();
		final QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		assertEquals(1, queryInfo.getTables().size());
		queryInfo.addTable(tableInfo2);
		final QueryItemCollection<QueryTableInfo, Table, TableName> lst = queryInfo
				.getTables();
		assertEquals(2, lst.size());

		assertEquals(tableInfo, lst.get(0));
		assertEquals(tableInfo2, lst.get(1));
	}

	@Test
	public void testListColumns() throws SQLDataException {
		assertEquals(0, queryInfo.listColumns(cName).size());
		assertEquals(0, queryInfo.listColumns(tName).size());
		assertEquals(0, queryInfo.listColumns(cName2).size());
		assertEquals(0, queryInfo.listColumns(tName2).size());
		assertEquals(0, queryInfo.listColumns(ItemName.WILD).size());

		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(0, queryInfo.listColumns(cName).size());
		assertEquals(0, queryInfo.listColumns(tName).size());
		assertEquals(1, queryInfo.listColumns(cName2).size());
		assertEquals(1, queryInfo.listColumns(tName2).size());
		assertEquals(1, queryInfo.listColumns(ItemName.WILD).size());

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());
		assertEquals(1, queryInfo.listColumns(cName).size());
		assertEquals(1, queryInfo.listColumns(tName).size());
		assertEquals(1, queryInfo.listColumns(cName2).size());
		assertEquals(1, queryInfo.listColumns(tName2).size());
		assertEquals(2, queryInfo.listColumns(ItemName.WILD).size());

	}

	@Test
	public void testListTables() throws SQLDataException {
		assertEquals(0, queryInfo.listTables(cName).size());
		assertEquals(0, queryInfo.listTables(tName).size());
		assertEquals(0, queryInfo.listTables(cName2).size());
		assertEquals(0, queryInfo.listTables(tName2).size());
		assertEquals(0, queryInfo.listTables(ItemName.WILD).size());

		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(0, queryInfo.listTables(cName).size());
		assertEquals(0, queryInfo.listTables(tName).size());
		assertEquals(0, queryInfo.listTables(cName2).size());
		assertEquals(0, queryInfo.listTables(tName2).size());
		assertEquals(0, queryInfo.listTables(ItemName.WILD).size());

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());
		assertEquals(1, queryInfo.listTables(cName).size());
		assertEquals(1, queryInfo.listTables(tName).size());
		assertEquals(0, queryInfo.listTables(cName2).size());
		assertEquals(0, queryInfo.listTables(tName2).size());
		assertEquals(1, queryInfo.listTables(ItemName.WILD).size());

		final ElementGroup queryElementGroup2 = new ElementGroup();
		final QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		queryInfo.addTable(tableInfo2);
		queryInfo.addDefinedColumns(Collections.emptyList());
		assertEquals(1, queryInfo.listTables(cName).size());
		assertEquals(1, queryInfo.listTables(tName).size());
		assertEquals(1, queryInfo.listTables(cName2).size());
		assertEquals(1, queryInfo.listTables(tName2).size());
		assertEquals(2, queryInfo.listTables(ItemName.WILD).size());
	}

	@Test
	public void testScanTablesForColumn() {
		final ColumnName cNameWild = new ColumnName(cName, NameSegments.FFFT);

		assertNull(queryInfo.scanTablesForColumn(cNameWild));
		assertNull(queryInfo.scanTablesForColumn(cName));
		assertNull(queryInfo.scanTablesForColumn(cName2));

		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertNull(queryInfo.scanTablesForColumn(cName));
		assertNotNull(queryInfo.scanTablesForColumn(cName2));
		assertNotNull(queryInfo.scanTablesForColumn(cNameWild));

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		assertNotNull(queryInfo.scanTablesForColumn(cName));
		assertNotNull(queryInfo.scanTablesForColumn(cName2));
		assertEquals(1, queryInfo.listTables(cName).size());
		assertEquals(0, queryInfo.listTables(cName2).size());

		try {
			queryInfo.scanTablesForColumn(cNameWild);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertTrue(e.getMessage()
					.startsWith("column was found in multiple"));
		}

		final ElementGroup queryElementGroup2 = new ElementGroup();
		final QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		queryInfo.addTable(tableInfo2);

		assertNotNull(queryInfo.scanTablesForColumn(cName));
		assertNotNull(queryInfo.scanTablesForColumn(cName2));

		assertEquals(1, queryInfo.listTables(cName).size());
		assertEquals(1, queryInfo.listTables(cName2).size());

		try {
			queryInfo.scanTablesForColumn(cNameWild);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertTrue(e.getMessage()
					.startsWith("column was found in multiple"));
		}
	}

	@Test
	public void testSetMinimumColumnSegments() throws SQLDataException {

		queryInfo.addColumn(new QueryColumnInfo(column2));

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());

		queryInfo.setMinimumColumnSegments();
		NameSegments expected = NameSegments.FFTF;
		for (final QueryTableInfo tableInfoChk : queryInfo.getTables()) {
			assertEquals(expected, tableInfoChk.getSegments());
		}

		expected = NameSegments.FFFT;
		for (final QueryColumnInfo columnInfoChk : queryInfo.getColumns()) {
			assertEquals(expected, columnInfoChk.getSegments());
		}

		final ElementGroup queryElementGroup2 = new ElementGroup();
		final QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		queryInfo.addTable(tableInfo2);

		queryInfo.setMinimumColumnSegments();

		expected = NameSegments.FTTF;
		for (final QueryTableInfo tableInfoChk : queryInfo.getTables()) {
			assertEquals(expected, tableInfoChk.getSegments());
		}

		expected = NameSegments.FTTT;
		for (final QueryColumnInfo columnInfoChk : queryInfo.getColumns()) {
			assertEquals(expected, columnInfoChk.getSegments());
		}
	}

	
	@Test
	public void testFindColumnByGUIDVar_String() throws SQLDataException {
		QueryColumnInfo colInfo = queryInfo
				.findColumnByGUID(cName);
		assertNull(colInfo);

		final ElementGroup queryElementGroup = new ElementGroup();
		final QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addDefinedColumns(Collections.emptyList());

		colInfo = queryInfo.findColumnByGUID(cName);
		assertNotNull(colInfo);
		assertEquals(cName.getGUID(), colInfo.getName().getGUID());
		assertEquals(cName.getGUID(), colInfo.getGUID());
	}

}
