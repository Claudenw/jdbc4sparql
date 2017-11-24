package org.xenei.jdbc4sparql.sparql.items;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLDataException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.sparql.QueryInfoSet;
import org.xenei.jdbc4sparql.utils.ElementExtractor;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;

public class QueryTableInfoTest {

	private QueryTableInfo tableInfo;
	private QueryInfoSet infoSet;
	private Table table;
	private TableName tableName;
	private ElementGroup queryElementGroup;

	@Before
	public void setup() {
		infoSet = new QueryInfoSet();
		queryElementGroup = new ElementGroup();
		table = mock(Table.class);
		tableName = new TableName("catalog", "schema", "table");
		when(table.getName()).thenReturn(tableName);
		tableInfo = new QueryTableInfo(infoSet, queryElementGroup, table, false);
	}

	@Test
	public void testSegments() {
		assertEquals("C:false S:true T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("schema.table", tableInfo.getName().getDBName());
		tableInfo.setSegments(NameSegments.CATALOG);
		assertEquals("C:true S:false T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("catalog.schema.table", tableInfo.getName().getDBName());
		tableInfo.setSegments(NameSegments.SCHEMA);
		assertEquals("C:false S:true T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("schema.table", tableInfo.getName().getDBName());
		tableInfo.setSegments(NameSegments.TABLE);
		assertEquals("C:false S:true T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("schema.table", tableInfo.getName().getDBName());
		tableInfo.setSegments(NameSegments.FFTT);
		assertEquals("C:false S:false T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("table", tableInfo.getName().getDBName());
		tableInfo.setSegments(NameSegments.TTTT);
		assertEquals("C:true S:true T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("catalog.schema.table", tableInfo.getName().getDBName());

	}

	@Test
	public void testGetName() {
		final ItemName name = tableInfo.getName();
		assertTrue(name == tableName);
	}

	@Test
	public void testGetVar() {
		final Var v = tableInfo.getVar();
		assertEquals(tableName.getGUID(), v.getName());
	}

	@Test
	public void testGetAlias() {
		final String varName = tableInfo.getName().getGUID();
		assertNotNull(tableInfo.getGUIDVar());
		assertEquals(varName, tableInfo.getGUIDVar().getVarName());
	}

	@Test
	public void testIsOptional() {
		assertFalse(tableInfo.isOptional());
		tableInfo.setOptional(true);
		assertTrue(tableInfo.isOptional());
	}

	@Test
	public void testAddColumnToQuery_Column() {
		Column column;
		ColumnName columnName;

		columnName = new ColumnName("catalog", "schema", "table", "column");
		column = mock(Column.class);
		when(column.getName()).thenReturn(columnName);

		final QueryColumnInfo columnInfo = tableInfo.addColumnToQuery(column);
		assertEquals(column.getName().getSPARQLName(), columnInfo.getName()
				.getSPARQLName());
	}

	@Test
	public void testAddColumnToQuery_Column_ColumnName_Optional() {
		Column column;
		ColumnName columnName;

		columnName = new ColumnName("catalog", "schema", "table", "column");
		column = mock(Column.class);
		when(column.getName()).thenReturn(columnName);

		final ColumnName columnName2 = new ColumnName("catalog", "schema",
				"table", "column2");

		final QueryColumnInfo columnInfo = tableInfo.addColumnToQuery(column,
				columnName2, true);
		assertEquals(columnName2.getSPARQLName(), columnInfo.getName()
				.getSPARQLName());

	}

	@Test
	public void testAddColumnToQuery_Column_Optional() {
		Column column;
		ColumnName columnName;

		columnName = new ColumnName("catalog", "schema", "table", "column");
		column = mock(Column.class);
		when(column.getName()).thenReturn(columnName);

		final QueryColumnInfo columnInfo = tableInfo.addColumnToQuery(column,
				true);
		assertEquals(column.getName().getSPARQLName(), columnInfo.getName()
				.getSPARQLName());
	}

	@Test
	public void testAddDefinedColumns() throws SQLDataException {
		when(table.getQuerySegmentFmt()).thenReturn("{ ?tbl <a> %s }");
		Column column;
		ColumnName columnName;

		columnName = new ColumnName("catalog", "schema", "table", "column");
		column = mock(Column.class);
		when(column.getName()).thenReturn(columnName);
		when(column.getQuerySegmentFmt()).thenReturn(" { %s <b> %s } ");
		when(column.hasQuerySegments()).thenReturn(true);

		final List<Column> cols = new ArrayList<Column>();
		cols.add(column);
		when(table.getColumns()).thenReturn(cols.iterator());

		tableInfo.addDefinedColumns(Collections.emptyList());

		final ElementExtractor extractor = new ElementExtractor(
				ElementPathBlock.class);
		extractor.visit(queryElementGroup);

		ElementPathBlock epb = (ElementPathBlock) extractor.getExtracted().get(
				0);
		TriplePath pth = epb.patternElts().next();
		assertEquals(Var.alloc("tbl"), pth.asTriple().getSubject());
		assertEquals(NodeFactory.createURI("a"), pth.asTriple().getPredicate());
		assertEquals(tableInfo.getGUIDVar(), pth.asTriple().getObject());

		epb = (ElementPathBlock) extractor.getExtracted().get(1);
		pth = epb.patternElts().next();
		assertEquals(tableInfo.getGUIDVar(), pth.asTriple().getSubject());
		assertEquals(NodeFactory.createURI("b"), pth.asTriple().getPredicate());
		assertEquals(Var.alloc( columnName.getGUID()), pth
				.asTriple().getObject());

	}

}
