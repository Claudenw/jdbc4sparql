package org.xenei.jdbc4sparql.sparql.items;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
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

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;

import static org.junit.Assert.*;

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
	public void testGetExpr() {
		assertNull(tableInfo.getExpr());
		tableInfo.setExpr(new NodeValueString("foo"));
		assertEquals(new NodeValueString("foo"), tableInfo.getExpr());
	}

	@Test
	public void testSegments() {
		assertEquals("C:false S:true T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("schema.table", tableInfo.getName().getDBName());
		tableInfo.setSegments(NameSegments.CATALOG);
		assertEquals("C:true S:false T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("table", tableInfo.getName().getDBName());
		tableInfo.setSegments(NameSegments.SCHEMA);
		assertEquals("C:false S:true T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("schema.table", tableInfo.getName().getDBName());
		tableInfo.setSegments(NameSegments.TABLE);
		assertEquals("C:false S:true T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("schema.table", tableInfo.getName().getDBName());
		tableInfo.setSegments(new NameSegments(false, false, true, true));
		assertEquals("C:false S:false T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("table", tableInfo.getName().getDBName());
		tableInfo.setSegments(new NameSegments(true, true, true, true));
		assertEquals("C:true S:true T:true C:false", tableInfo.getSegments()
				.toString());
		assertEquals("schema.table", tableInfo.getName().getDBName());

	}

	@Test
	public void testGetName() {
		ItemName name = tableInfo.getName();
		assertTrue(name == tableName);
	}

	@Test
	public void testGetVar() {
		String dbName = "schema" + NameUtils.SPARQL_DOT + "table";
		Var v = tableInfo.getVar();
		assertEquals(dbName, v.getName());
	}

	@Test
	public void testGetAlias() {
		String varName = tableInfo.getName().getGUID();
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

		QueryColumnInfo columnInfo = tableInfo.addColumnToQuery(column);
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

		ColumnName columnName2 = new ColumnName("catalog", "schema", "table",
				"column2");

		QueryColumnInfo columnInfo = tableInfo.addColumnToQuery(column,
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

		QueryColumnInfo columnInfo = tableInfo.addColumnToQuery(column, true);
		assertEquals(column.getName().getSPARQLName(), columnInfo.getName()
				.getSPARQLName());
	}

	@Test
	public void testAddRequiredColumns() {
		when(table.getQuerySegmentFmt()).thenReturn("{ ?tbl <a> %s }");
		Column column;
		ColumnName columnName;

		columnName = new ColumnName("catalog", "schema", "table", "column");
		column = mock(Column.class);
		when(column.getName()).thenReturn(columnName);
		when(column.getQuerySegmentFmt()).thenReturn(" { %s <b> %s } ");

		List<Column> cols = new ArrayList<Column>();
		cols.add(column);
		when(table.getColumns()).thenReturn(cols.iterator());

		tableInfo.addRequiredColumns();

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

}
