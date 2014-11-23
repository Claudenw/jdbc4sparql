package org.xenei.jdbc4sparql.sparql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.SchemaName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.utils.ElementExtractor;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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

	@Before
	public void setup() {
		queryInfo = new QueryInfoSet();

		column = mock(Column.class);
		cName = new ColumnName("catalog", "schema", "table", "column");
		when(column.getName()).thenReturn(cName);
		when(column.getQuerySegmentFmt()).thenReturn(" { %s <b> %s } ");

		table = mock(Table.class);
		tName = new TableName("catalog", "schema", "table");
		when(table.getName()).thenReturn(tName);
		when(table.getQuerySegmentFmt()).thenReturn("{ ?tbl <a> %s }");
		when(table.getColumn(eq("column"))).thenReturn(column);

		List<Column> cols = new ArrayList<Column>();
		cols.add(column);
		when(table.getColumns()).thenReturn(cols.iterator());

		column2 = mock(Column.class);
		cName2 = new ColumnName("catalog", "schema2", "table", "column");
		when(column2.getName()).thenReturn(cName2);
		when(column2.getQuerySegmentFmt()).thenReturn(" { %s <b> %s } ");

		table2 = mock(Table.class);
		tName2 = new TableName("catalog", "schema2", "table");
		when(table2.getName()).thenReturn(tName2);
		when(table2.getQuerySegmentFmt()).thenReturn("{ ?tbl <a> %s }");
		when(table2.getColumn(eq("column"))).thenReturn(column2);

		List<Column> cols2 = new ArrayList<Column>();
		cols2.add(column2);
		when(table2.getColumns()).thenReturn(cols2.iterator());

	}

	@Test
	public void testAddColumn() {
		QueryColumnInfo columnInfo = new QueryColumnInfo(column);
		queryInfo.addColumn(columnInfo);
		QueryColumnInfo found = queryInfo.findColumnByGUID(cName);
		assertEquals(found, columnInfo);
	}

	@Test
	public void testAddRequiredColumns() {
		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();

		ElementExtractor extractor = new ElementExtractor(
				ElementPathBlock.class);
		extractor.visit(queryElementGroup);

		ElementPathBlock epb = (ElementPathBlock) extractor.getExtracted().get(
				0);
		TriplePath pth = epb.patternElts().next();
		assertEquals(Var.alloc("tbl"), pth.asTriple().getSubject());
		assertEquals(NodeFactory.createURI("a"), pth.asTriple().getPredicate());
		assertEquals(Var.alloc("schema" + NameUtils.SPARQL_DOT + "table"), pth
				.asTriple().getObject());

		epb = (ElementPathBlock) extractor.getExtracted().get(1);
		pth = epb.patternElts().next();
		assertEquals(Var.alloc("schema" + NameUtils.SPARQL_DOT + "table"), pth
				.asTriple().getSubject());
		assertEquals(NodeFactory.createURI("b"), pth.asTriple().getPredicate());
		assertEquals(Var.alloc("v_906819fe_e4e6_30eb_8431_4483a755c4f4"), pth
				.asTriple().getObject());

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
		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);

		assertEquals(new NameSegments(true, true, true, false),
				tableInfo.getSegments());
		assertEquals(1, queryInfo.getTables().size());
		assertEquals(tableInfo.getGUID(), queryInfo.getTables().iterator()
				.next().getGUID());

		QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup, table2, false);
		queryInfo.addTable(tableInfo2);

		assertEquals(new NameSegments(true, true, true, false),
				tableInfo.getSegments());
		assertEquals(new NameSegments(true, true, true, false),
				tableInfo2.getSegments());
		assertEquals(2, queryInfo.getTables().size());
		Set<String> guids = new HashSet<String>();
		for (QueryTableInfo qti : queryInfo.getTables()) {
			guids.add(qti.getGUID());
		}
		assertEquals(2, guids.size());

		assertTrue(guids.contains(tableInfo.getGUID()));
		assertTrue(guids.contains(tableInfo2.getGUID()));
	}

	@Test
	public void testContainsColumn() {
		assertFalse(queryInfo.containsColumn(cName2));
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertTrue(queryInfo.containsColumn(cName2));

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
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
	public void testFindColumn() {
		assertNull(queryInfo.findColumn(cName2));
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(cName2, queryInfo.findColumn(cName2).getName());
		assertEquals(cName2, queryInfo.findColumn(new ColumnName(cName2))
				.getName());

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		assertEquals(cName, queryInfo.findColumn(cName).getName());

		assertNull(queryInfo.findColumn(new ColumnName("catalog", "schema2",
				"table", "column2")));

	}

	@Test
	public void testFindColumnByGUID() {
		assertNull(queryInfo.findColumnByGUID(cName2));
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(cName2, queryInfo.findColumnByGUID(cName2).getName());
		assertEquals(cName2, queryInfo.findColumnByGUID(new ColumnName(cName2))
				.getName());

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		assertEquals(cName, queryInfo.findColumnByGUID(cName).getName());

		assertNull(queryInfo.findColumnByGUID(new ColumnName("catalog",
				"schema2", "table", "column2")));

	}

	@Test
	public void testGetColumnByName_ColumnName() {
		try {
			queryInfo.getColumn(cName2);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {

		}
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(cName2, queryInfo.getColumn(cName2).getName());
		assertEquals(cName2, queryInfo.getColumn(new ColumnName(cName2))
				.getName());

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		assertEquals(cName, queryInfo.getColumn(cName).getName());

		try {
			queryInfo.getColumn(new ColumnName("catalog", "schema2", "table",
					"column2"));
		} catch (IllegalArgumentException expected) {

		}
	}

	@Test
	public void testGetColumnByName_Var() {
		Var v = Var.alloc(cName.getSPARQLName());
		Var v2 = Var.alloc(cName2.getSPARQLName());
		try {
			queryInfo.getColumn(v2);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {

		}
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(cName2, queryInfo.getColumn(v2).getName());

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		assertEquals(cName, queryInfo.getColumn(v).getName());

		try {

			queryInfo.getColumn(Var.alloc("Dummy"));
		} catch (IllegalArgumentException expected) {

		}
	}

	@Test
	public void testGetColumnByNode() {
		Node n = Var.alloc(cName.getSPARQLName()).asNode();
		Node n2 = Var.alloc(cName2.getSPARQLName()).asNode();
		try {
			queryInfo.getColumn(n2);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {

		}
		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(cName2, queryInfo.getColumn(n2).getName());

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		assertEquals(cName, queryInfo.getColumn(n).getName());

		try {
			queryInfo.getColumn(NodeFactory.createAnon());
		} catch (IllegalArgumentException expected) {
		}

		try {
			queryInfo.getColumn(NodeFactory.createLiteral("foo"));
		} catch (IllegalArgumentException expected) {
		}

		try {
			queryInfo.getColumn(NodeFactory.createURI("dummy"));
		} catch (IllegalArgumentException expected) {
		}

		try {
			queryInfo.getColumn(NodeFactory.createVariable("dummy"));
		} catch (IllegalArgumentException expected) {
		}

		try {
			queryInfo.getColumn(Node.ANY);
		} catch (IllegalArgumentException expected) {
		}
	}

	@Test
	public void testGetColumnIndex() {

		assertEquals(-1, queryInfo.getColumnIndex(cName2));

		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertEquals(0, queryInfo.getColumnIndex(cName2));
		assertEquals(0, queryInfo.getColumnIndex(new ColumnName(cName2)));

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		assertEquals(1, queryInfo.getColumnIndex(cName));

		assertEquals(-1, queryInfo.getColumnIndex(new ColumnName("catalog",
				"schema2", "table", "column2")));
	}

	@Test
	public void testGetColumns() {
		assertEquals(0, queryInfo.getColumns().size());
		QueryColumnInfo colInfo1 = new QueryColumnInfo(column2);
		queryInfo.addColumn(colInfo1);
		assertEquals(1, queryInfo.getColumns().size());

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		List<QueryColumnInfo> cols = queryInfo.getColumns();
		assertEquals(2, cols.size());
		assertEquals(colInfo1, cols.get(0));
		assertEquals(queryInfo.getColumn(cName), cols.get(1));
	}

	@Test
	public void testGetTable() {
		assertNull(queryInfo.getTable(table.getName()));
		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);

		assertEquals(tableInfo, queryInfo.getTable(table.getName()));
		assertNull(queryInfo.getTable(table2.getName()));

		ElementGroup queryElementGroup2 = new ElementGroup();
		QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		queryInfo.addTable(tableInfo2);

		assertEquals(tableInfo, queryInfo.getTable(table.getName()));
		assertEquals(tableInfo2, queryInfo.getTable(table2.getName()));

		assertNull(queryInfo.getTable(new TableName("catalog", "schema",
				"table3")));

	}

	@Test
	public void testGetTableByNode() {
		Node n = Var.alloc(tName.getSPARQLName()).asNode();
		Node n2 = Var.alloc(tName2.getSPARQLName()).asNode();
		assertNull(queryInfo.getTable(n));

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		assertNull(queryInfo.getTable(n));

		queryInfo.addTable(tableInfo);
		assertEquals(tableInfo, queryInfo.getTable(n));

		ElementGroup queryElementGroup2 = new ElementGroup();
		QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		assertNull(queryInfo.getTable(n2));
		queryInfo.addTable(tableInfo2);
		assertEquals(tableInfo2, queryInfo.getTable(n2));

		assertNull(queryInfo.getTable(NodeFactory.createAnon()));
		assertNull(queryInfo.getTable(NodeFactory.createLiteral("foo")));
		assertNull(queryInfo.getTable(NodeFactory.createURI("dummy")));
		assertNull(queryInfo.getTable(NodeFactory.createVariable("dummy")));
		assertNull(queryInfo.getTable(Node.ANY));
	}

	@Test
	public void testGetTables() {
		assertEquals(0, queryInfo.getTables().size());

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		assertEquals(0, queryInfo.getTables().size());
		queryInfo.addTable(tableInfo);
		assertEquals(1, queryInfo.getTables().size());

		ElementGroup queryElementGroup2 = new ElementGroup();
		QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		assertEquals(1, queryInfo.getTables().size());
		queryInfo.addTable(tableInfo2);
		List<QueryTableInfo> lst = queryInfo.getTables();
		assertEquals(2, lst.size());

		assertEquals(tableInfo, lst.get(0));
		assertEquals(tableInfo2, lst.get(1));
	}

	@Test
	public void testListColumns() {
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

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		assertEquals(1, queryInfo.listColumns(cName).size());
		assertEquals(1, queryInfo.listColumns(tName).size());
		assertEquals(1, queryInfo.listColumns(cName2).size());
		assertEquals(1, queryInfo.listColumns(tName2).size());
		assertEquals(2, queryInfo.listColumns(ItemName.WILD).size());

	}

	@Test
	public void testListTables() {
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

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();
		assertEquals(1, queryInfo.listTables(cName).size());
		assertEquals(1, queryInfo.listTables(tName).size());
		assertEquals(0, queryInfo.listTables(cName2).size());
		assertEquals(0, queryInfo.listTables(tName2).size());
		assertEquals(1, queryInfo.listTables(ItemName.WILD).size());

		ElementGroup queryElementGroup2 = new ElementGroup();
		QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		queryInfo.addTable(tableInfo2);
		queryInfo.addRequiredColumns();
		assertEquals(1, queryInfo.listTables(cName).size());
		assertEquals(1, queryInfo.listTables(tName).size());
		assertEquals(1, queryInfo.listTables(cName2).size());
		assertEquals(1, queryInfo.listTables(tName2).size());
		assertEquals(2, queryInfo.listTables(ItemName.WILD).size());
	}

	@Test
	public void testScanTablesForColumn() {
		ColumnName cNameWild = new ColumnName(cName, new NameSegments(false,
				false, false, true));

		assertNull(queryInfo.scanTablesForColumn(cNameWild));
		assertNull(queryInfo.scanTablesForColumn(cName));
		assertNull(queryInfo.scanTablesForColumn(cName2));

		queryInfo.addColumn(new QueryColumnInfo(column2));
		assertNull(queryInfo.scanTablesForColumn(cName));
		assertNotNull(queryInfo.scanTablesForColumn(cName2));
		assertNotNull(queryInfo.scanTablesForColumn(cNameWild));

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		assertNotNull(queryInfo.scanTablesForColumn(cName));
		assertNotNull(queryInfo.scanTablesForColumn(cName2));
		assertEquals(1, queryInfo.listTables(cName).size());
		assertEquals(0, queryInfo.listTables(cName2).size());

		try {
			queryInfo.scanTablesForColumn(cNameWild);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertTrue(e.getMessage()
					.startsWith("column was found in multiple"));
		}

		ElementGroup queryElementGroup2 = new ElementGroup();
		QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		queryInfo.addTable(tableInfo2);

		assertNotNull(queryInfo.scanTablesForColumn(cName));
		assertNotNull(queryInfo.scanTablesForColumn(cName2));

		assertEquals(1, queryInfo.listTables(cName).size());
		assertEquals(1, queryInfo.listTables(cName2).size());

		try {
			queryInfo.scanTablesForColumn(cNameWild);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertTrue(e.getMessage()
					.startsWith("column was found in multiple"));
		}
	}

	@Test
	public void testSetMinimumColumnSegments() {

		queryInfo.addColumn(new QueryColumnInfo(column2));

		ElementGroup queryElementGroup = new ElementGroup();
		QueryTableInfo tableInfo = new QueryTableInfo(queryInfo,
				queryElementGroup, table, false);
		queryInfo.addTable(tableInfo);
		queryInfo.addRequiredColumns();

		queryInfo.setMinimumColumnSegments();
		NameSegments expected = new NameSegments(false, false, true, false);
		for (QueryTableInfo tableInfoChk : queryInfo.getTables()) {
			assertEquals(expected, tableInfoChk.getSegments());
		}

		expected = new NameSegments(false, false, false, true);
		for (QueryColumnInfo columnInfoChk : queryInfo.getColumns()) {
			assertEquals(expected, columnInfoChk.getSegments());
		}

		ElementGroup queryElementGroup2 = new ElementGroup();
		QueryTableInfo tableInfo2 = new QueryTableInfo(queryInfo,
				queryElementGroup2, table2, false);
		queryInfo.addTable(tableInfo2);

		queryInfo.setMinimumColumnSegments();

		expected = new NameSegments(false, true, true, false);
		for (QueryTableInfo tableInfoChk : queryInfo.getTables()) {
			assertEquals(expected, tableInfoChk.getSegments());
		}

		expected = new NameSegments(false, true, true, true);
		for (QueryColumnInfo columnInfoChk : queryInfo.getColumns()) {
			assertEquals(expected, columnInfoChk.getSegments());
		}

	}

}
