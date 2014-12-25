package org.xenei.jdbc4sparql.sparql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.SchemaName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryItemCollection;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.FunctionColumn;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.FunctionColumnDef;
import org.xenei.jdbc4sparql.utils.ElementExtractor;
import org.xenei.jdbc4sparql.utils.ExpressionExtractor;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.expr.E_UUID;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCount;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;

public class SparqlQueryBuilderTest {

	private Map<String, Catalog> catalogs;

	private SparqlParser parser;

	private SparqlQueryBuilder builder;

	private VirtualCatalog vCat;

	private Catalog catalog;
	private CatalogName catName;

	private Schema schema;
	private SchemaName sName;

	private Table table;
	private TableName tName;

	private Column column2;
	private ColumnName col2Name;

	private Column column;
	private ColumnName colName;

	private ColumnDef colDef;

	private List<Column> cols;

	@Before
	public void setup() {
		catName = new CatalogName("TestCatalog");
		sName = catName.getSchemaName("testSchema");
		tName = sName.getTableName("testTable");
		colName = tName.getColumnName("testColumn");
		col2Name = tName.getColumnName("testColumn2");

		parser = new SparqlParserImpl();
		catalog = mock(Catalog.class);
		when(catalog.getName()).thenReturn(catName);
		when(catalog.getShortName()).thenReturn(catName.getShortName());

		catalogs = new HashMap<String, Catalog>();
		catalogs.put("TestCatalog", catalog);
		vCat = new VirtualCatalog();
		catalogs.put(VirtualCatalog.NAME, vCat);

		schema = mock(Schema.class);
		when(schema.getName()).thenReturn(sName);

		table = mock(Table.class);
		when(table.getName()).thenReturn(tName);
		when(table.hasQuerySegments()).thenReturn(true);
		when(table.getQuerySegmentFmt()).thenReturn("%1$s <a> 'table' . ");

		colDef = mock(ColumnDef.class);
		when(colDef.getType()).thenReturn(java.sql.Types.VARCHAR);

		column = mock(Column.class);
		when(column.getName()).thenReturn(colName);
		when(column.hasQuerySegments()).thenReturn(true);
		when(column.getQuerySegmentFmt()).thenReturn("%1$s <of> %2$s . ");
		when(column.getColumnDef()).thenReturn(colDef);
		when(table.getColumn(eq(colName.getShortName()))).thenReturn(column);

		column2 = mock(Column.class);
		when(column2.getName()).thenReturn(col2Name);
		when(column2.hasQuerySegments()).thenReturn(true);
		when(column2.getQuerySegmentFmt()).thenReturn("%1$s <of> %2$s . ");
		when(column2.getColumnDef()).thenReturn(colDef);
		when(column2.isOptional()).thenReturn(true);
		when(table.getColumn(eq(col2Name.getShortName()))).thenReturn(column2);

		cols = new ArrayList<Column>();
		cols.add(column);
		cols.add(column2);
		when(table.getColumns()).thenAnswer(new ColumnAnswer(cols));
		when(table.getColumnList()).thenReturn(cols);

		builder = new SparqlQueryBuilder(catalogs, parser, catalog, schema);
	}

	private static SparqlQueryBuilder createBuilder(final String tableName) {
		final CatalogName catName = new CatalogName("TestCatalog");
		final SchemaName sName = catName.getSchemaName("testSchema");
		final TableName tName = sName.getTableName(tableName);
		final ColumnName colName = tName.getColumnName("testColumn");
		final ColumnName col2Name = tName.getColumnName("testColumn2");

		final SparqlParserImpl parser = new SparqlParserImpl();
		final RdfCatalog catalog = mock(RdfCatalog.class);
		when(catalog.getName()).thenReturn(catName);
		when(catalog.getShortName()).thenReturn(catName.getShortName());

		final Map<String, Catalog> catalogs = new HashMap<String, Catalog>();
		catalogs.put("TestCatalog", catalog);

		final Schema schema = mock(Schema.class);
		when(schema.getName()).thenReturn(sName);

		final Table table = mock(Table.class);
		when(table.getName()).thenReturn(tName);
		when(table.hasQuerySegments()).thenReturn(true);
		when(table.getQuerySegmentFmt()).thenReturn("%1$s <a> 'table' . ");

		final ColumnDef colDef = mock(ColumnDef.class);
		when(colDef.getType()).thenReturn(java.sql.Types.VARCHAR);

		final Column column = mock(Column.class);
		when(column.getName()).thenReturn(colName);
		when(column.hasQuerySegments()).thenReturn(true);
		when(column.getQuerySegmentFmt()).thenReturn("%1$s <of> %2$s . ");
		when(column.getColumnDef()).thenReturn(colDef);
		when(table.getColumn(eq(colName.getShortName()))).thenReturn(column);

		final Column column2 = mock(Column.class);
		when(column2.getName()).thenReturn(col2Name);
		when(column2.hasQuerySegments()).thenReturn(true);
		when(column2.getQuerySegmentFmt()).thenReturn("%1$s <of> %2$s . ");
		when(column2.getColumnDef()).thenReturn(colDef);
		when(column2.isOptional()).thenReturn(true);
		when(table.getColumn(eq(col2Name.getShortName()))).thenReturn(column2);

		final ArrayList<Column> cols = new ArrayList<Column>();
		cols.add(column);
		cols.add(column2);
		when(table.getColumns()).thenAnswer(new ColumnAnswer(cols));

		final SparqlQueryBuilder builder = new SparqlQueryBuilder(catalogs,
				parser, catalog, schema);
		builder.addTable(table, tName, false);
		return builder;
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

	private Query getQuery() throws Exception {
		final Field f = SparqlQueryBuilder.class.getDeclaredField("query");
		f.setAccessible(true);
		return (Query) f.get(builder);
	}

	private QueryInfoSet getInfoSet() throws Exception {
		final Field f = SparqlQueryBuilder.class.getDeclaredField("infoSet");
		f.setAccessible(true);
		return (QueryInfoSet) f.get(builder);
	}

	@Test
	public void testAddTable_Required() throws Exception {
		builder.addTable(table, tName, false);

		final QueryInfoSet infoSet = getInfoSet();
		final QueryItemCollection<QueryTableInfo, Table, TableName> tableInfoList = infoSet
				.getTables();
		assertEquals(1, tableInfoList.size());
		final QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals(tName, tableInfo.getName());
		assertEquals(table, tableInfo.getTable());
		assertEquals(false, tableInfo.isOptional());
		assertEquals(table.getName().getGUID(), tName.getGUID());
	}

	@Test
	public void testAddTable_WithAlias() throws Exception {
		final TableName alias = new TableName(catName.getCatalog(),
				sName.getSchema(), "tableAlias");
		builder.addTable(table, alias, false);

		final QueryInfoSet infoSet = getInfoSet();
		final QueryItemCollection<QueryTableInfo, Table, TableName> tableInfoList = infoSet
				.getTables();
		assertEquals(1, tableInfoList.size());
		final QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals(alias, tableInfo.getName());
		assertEquals(table, tableInfo.getTable());
		assertEquals(false, tableInfo.isOptional());
		assertEquals(table.getName().getGUID(), tName.getGUID());
		// alias name GUID does not match
		assertNotEquals(table.getName().getGUID(), alias.getGUID());
		// but GUID for table associated with alias does.
		final QueryTableInfo aliasTable = infoSet.getTable(alias);
		assertEquals(table.getName().getGUID(), aliasTable.getGUID());

	}

	@Test
	public void testAddTable_Optional() throws Exception {
		builder.addTable(table, tName, true);

		final QueryInfoSet infoSet = getInfoSet();
		final QueryItemCollection<QueryTableInfo, Table, TableName> tableInfoList = infoSet
				.getTables();
		assertEquals(1, tableInfoList.size());
		final QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals(tName, tableInfo.getName());
		assertEquals(table, tableInfo.getTable());
		assertEquals(true, tableInfo.isOptional());
		assertEquals(table.getName().getGUID(), tName.getGUID());
	}

	@Test
	public void testAddRequiredColumns() throws Exception {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();

		final QueryInfoSet infoSet = getInfoSet();
		final QueryItemCollection<QueryTableInfo, Table, TableName> tableInfoList = infoSet
				.getTables();
		assertEquals(1, tableInfoList.size());
		final QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals(tName, tableInfo.getName());
		assertEquals(table, tableInfo.getTable());

		final QueryItemCollection<QueryColumnInfo, Column, ColumnName> columnInfoList = infoSet
				.getColumns();
		assertEquals(1, columnInfoList.size());
		final QueryColumnInfo columnInfo = columnInfoList.get(0);
		assertEquals(colName, columnInfo.getName());
		assertEquals(column, columnInfo.getColumn());

		final Query query = getQuery();
		final ElementExtractor extractor = new ElementExtractor(
				ElementPathBlock.class);
		query.getQueryPattern().visit(extractor);
		final List<Element> lst = extractor.getExtracted();
		assertEquals(1, lst.size());

		final Var tVar = Var.alloc(tName.getGUID());
		final List<TriplePath> etb = ((ElementPathBlock) lst.get(0))
				.getPattern().getList();
		assertEquals(2, etb.size());
		TriplePath t = new TriplePath(new Triple(tVar,
				NodeFactory.createURI("a"), NodeFactory.createLiteral("table")));
		assertTrue(etb.contains(t));
		t = new TriplePath(new Triple(tVar, NodeFactory.createURI("of"),
				Var.alloc(colName.getGUID())));
		assertTrue(etb.contains(t));
	}

	@Test
	public void testAddColumnToQuery() throws Exception {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		builder.setSegmentCount();
		builder.addColumnToQuery(col2Name, true);
		QueryTableInfo tableInfo = builder.getTable(tName);
		builder.addVar(col2Name);

		final QueryInfoSet infoSet = getInfoSet();
		final QueryItemCollection<QueryTableInfo, Table, TableName> tableInfoList = infoSet
				.getTables();
		assertEquals(1, tableInfoList.size());
		tableInfo = tableInfoList.get(0);
		assertEquals(tName, tableInfo.getName());
		assertEquals(table, tableInfo.getTable());

		final QueryItemCollection<QueryColumnInfo, Column, ColumnName> columnInfoList = new QueryItemCollection<QueryColumnInfo, Column, ColumnName>(
				infoSet.getColumns());
		assertEquals(2, columnInfoList.size());

		assertTrue(columnInfoList.contains(colName));
		assertTrue(columnInfoList.contains(column));

		assertTrue(columnInfoList.contains(col2Name));
		assertTrue(columnInfoList.contains(column2));

		final Query query = builder.build();

		final ElementExtractor extractor = new ElementExtractor(
				ElementPathBlock.class);
		query.getQueryPattern().visit(extractor);
		List<Element> lst = extractor.getExtracted();
		assertEquals(2, lst.size());

		List<TriplePath> etb = ((ElementPathBlock) lst.get(0)).getPattern()
				.getList();
		assertEquals(2, etb.size());
		final Var tVar = Var.alloc(tName.getGUID());
		TriplePath t = new TriplePath(new Triple(tVar,
				NodeFactory.createURI("a"), NodeFactory.createLiteral("table")));
		assertTrue(etb.contains(t));
		t = new TriplePath(new Triple(tVar, NodeFactory.createURI("of"),
				Var.alloc(colName.getGUID())));
		assertTrue(etb.contains(t));

		etb = ((ElementPathBlock) lst.get(1)).getPattern().getList();
		assertEquals(1, etb.size());
		t = new TriplePath(new Triple(tVar, NodeFactory.createURI("of"),
				Var.alloc(col2Name.getGUID())));
		assertTrue(etb.contains(t));

		query.getQueryPattern().visit(
				extractor.setMatchType(ElementFilter.class).reset());
		lst = extractor.getExtracted();
		assertEquals(1, lst.size());

		final ExpressionExtractor eExtractor = new ExpressionExtractor(
				CheckTypeF.class);
		((ElementFilter) lst.get(0)).getExpr().visit(eExtractor);
		final List<Expr> eExpr = eExtractor.getExtracted();
		assertEquals(2, eExpr.size());

		// ElementBind
		query.getQueryPattern().visit(
				extractor.setMatchType(ElementBind.class).reset());
		lst = extractor.getExtracted();
		assertEquals(1, lst.size());

	}

	@Test
	public void testAddAlias() throws Exception {
		final ColumnName alias = new ColumnName("testCatalog", "testSchema",
				"testTable", "alias");
		alias.setUsedSegments(NameSegments.FFFT);

		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		builder.setSegmentCount();
		builder.addAlias(col2Name, alias);
		builder.addVar(alias);

		final QueryInfoSet infoSet = getInfoSet();
		final QueryItemCollection<QueryTableInfo, Table, TableName> tableInfoList = infoSet
				.getTables();
		assertEquals(1, tableInfoList.size());
		final QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals(tName, tableInfo.getName());
		assertEquals(table, tableInfo.getTable());

		final QueryItemCollection<QueryColumnInfo, Column, ColumnName> columnInfoList = new QueryItemCollection<QueryColumnInfo, Column, ColumnName>(
				infoSet.getColumns());
		assertEquals(3, columnInfoList.size());

		assertTrue(columnInfoList.contains(colName));
		assertTrue(columnInfoList.contains(column));

		assertTrue(columnInfoList.contains(alias));
		assertTrue(columnInfoList.contains(column2));

		assertTrue(columnInfoList.contains(col2Name));

		final Query query = builder.build();

		final ElementExtractor extractor = new ElementExtractor(
				ElementBind.class);
		query.getQueryPattern().visit(extractor);
		final List<Element> lst = extractor.getExtracted();
		assertEquals(1, lst.size());
		assertEquals("alias", ((ElementBind) lst.get(0)).getVar().getName());
	}

	@Test
	public void testAddFilter() throws Exception {
		final Query query = getQuery();
		assertNull(query.getQueryPattern());

		final Expr expr = new E_UUID();
		builder.addFilter(expr);

		final ElementExtractor extractor = new ElementExtractor(
				ElementFilter.class);
		query.getQueryPattern().visit(extractor);
		final List<Element> eLst = extractor.getExtracted();
		assertEquals(1, eLst.size());
	}

	@Test
	public void testAddColumn() throws Exception {
		QueryColumnInfo columnInfo = null;

		try {
			columnInfo = builder.addColumn(col2Name, true);
			fail("Should have thrown SQLException");
		} catch (final SQLException e) {
			// expected
		}
		final QueryTableInfo tableInfo = builder.addTable(table, tName, false);
		columnInfo = builder.addColumn(col2Name, true);
		assertNotNull(columnInfo);
		assertEquals(col2Name, columnInfo.getName());
		final QueryInfoSet infoSet = getInfoSet();
		assertNotNull(infoSet.getColumn(col2Name));
		final Query query = getQuery();
		final ElementExtractor extractor = new ElementExtractor(
				ElementOptional.class);
		query.getQueryPattern().visit(extractor);
		List<Element> eLst = extractor.getExtracted();
		assertEquals(1, eLst.size());
		eLst.get(0).visit(
				extractor.reset().setMatchType(ElementPathBlock.class));
		eLst = extractor.getExtracted();
		assertEquals(1, eLst.size());
		final ElementPathBlock epb = (ElementPathBlock) eLst.get(0);
		final List<TriplePath> tLst = epb.getPattern().getList();
		assertEquals(1, tLst.size());
		final Triple t = tLst.get(0).asTriple();
		final Triple t2 = new Triple(tableInfo.getGUIDVar(),
				NodeFactory.createURI("of"), columnInfo.getGUIDVar());
		assertEquals(t2, t);
	}

	@Test
	public void testAddGroupBy() throws Exception {
		final Query query = getQuery();
		assertEquals(0, query.getGroupBy().size());

		final Expr expr = new E_UUID();
		;
		builder.addGroupBy(expr);

		final VarExprList eLst = query.getGroupBy();
		assertEquals(1, eLst.size());
		final Var v = eLst.getVars().get(0);
		final Expr e = eLst.getExpr(v);
		assertEquals(expr, e);

	}

	@Test
	public void testAddOrderBy() throws Exception {
		final Query query = getQuery();
		assertNull(query.getOrderBy());

		final Expr expr = new E_UUID();
		builder.addOrderBy(expr, true);

		final List<SortCondition> eLst = query.getOrderBy();
		assertEquals(1, eLst.size());
		final SortCondition sc = eLst.get(0);
		assertEquals(expr, sc.getExpression());
		assertEquals(Query.ORDER_ASCENDING, sc.getDirection());
	}

	@Test
	public void testAddUnion() throws Exception {
		final List<SparqlQueryBuilder> builders = new ArrayList<SparqlQueryBuilder>();
		final SparqlQueryBuilder builder1 = createBuilder("table1");
		builder1.addRequiredColumns();
		builder1.setAllColumns();

		final SparqlQueryBuilder builder2 = createBuilder("table2");
		builder2.addRequiredColumns();
		builder2.setAllColumns();

		builders.add(builder1);
		builders.add(builder2);

		builder.addUnion(builders);

		final Query query = getQuery();
		final ElementExtractor extractor = new ElementExtractor(
				ElementUnion.class);
		query.getQueryPattern().visit(extractor);
		List<Element> lst = extractor.getExtracted();
		assertEquals(1, lst.size());

		query.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementSubQuery.class));
		lst = extractor.getExtracted();
		assertEquals(2, lst.size());

	}

	@Test
	public void testUsing() throws Exception {
		// setup a second table
		final TableName tName2 = sName.getTableName("testTable2");

		final Table table2 = mock(Table.class);
		when(table2.getName()).thenReturn(tName2);
		when(table2.getQuerySegmentFmt()).thenReturn("%1$s <a> 'table' . ");
		when(table2.getColumn(eq(colName.getShortName()))).thenReturn(column);
		when(table2.getColumn(eq(col2Name.getShortName()))).thenReturn(column2);

		cols = new ArrayList<Column>();
		cols.add(column);
		cols.add(column2);
		when(table2.getColumns()).thenAnswer(new ColumnAnswer(cols));

		// setup complete

		builder.addTable(table, tName, false);
		builder.addTable(table2, tName2, false);
		builder.addUsing(colName.getShortName());
		builder.addRequiredColumns();
		builder.setAllColumns();

		final Field f = SparqlQueryBuilder.class
				.getDeclaredField("columnsInUsing");
		f.setAccessible(true);
		final List<String> inUsing = (List<String>) f.get(builder);
		assertEquals(1, inUsing.size());
		assertEquals(colName.getShortName(), inUsing.get(0));

		final Query query = builder.build();

		// fail("incomplete Test");
	}

	@Test
	public void testAddVar_ColumnName() throws Exception {
		final Query query = getQuery();
		assertEquals(0, query.getProjectVars().size());

		builder.addTable(table, tName, true);
		builder.addVar(colName);

		final List<Var> vLst = query.getProjectVars();
		assertEquals(1, vLst.size());
		assertEquals(Var.alloc(colName.getSPARQLName()), vLst.get(0));

		final VarExprList eLst = query.getProject();
		assertEquals(1, eLst.size());
		final Var v = eLst.getVars().get(0);
		final Expr e = eLst.getExpr(v);
		assertNull(e);
		assertEquals(Var.alloc(colName.getSPARQLName()), v);
	}

	@Test
	public void testAddVar_Expr_String() throws Exception {
		final Query query = getQuery();
		assertEquals(0, query.getProjectVars().size());

		final Expr expr = new E_UUID();
		builder.addVar(expr, "foo");

		final List<Var> vLst = query.getProjectVars();
		assertEquals(1, vLst.size());
		assertEquals(Var.alloc("foo"), vLst.get(0));

		final VarExprList eLst = query.getProject();
		assertEquals(1, eLst.size());
		final Var v = eLst.getVars().get(0);
		final Expr e = eLst.getExpr(v);
		assertEquals(expr, e);
		assertEquals(Var.alloc("foo"), v);
	}

	@Test
	public void testAddVar_Expr_ColumnName() throws Exception {
		final Query query = getQuery();
		assertEquals(0, query.getProjectVars().size());

		final Expr expr = new E_UUID();
		final ColumnName alias = new ColumnName("", "", "", "alias");
		alias.setUsedSegments(NameSegments.FFFT);
		builder.registerFunctionColumn(alias, java.sql.Types.INTEGER);
		builder.addVar(expr, alias);

		final List<Var> vLst = query.getProjectVars();
		assertEquals(1, vLst.size());
		assertEquals(Var.alloc(alias.getSPARQLName()), vLst.get(0));

		final VarExprList eLst = query.getProject();
		assertEquals(1, eLst.size());
		final Var v = eLst.getVars().get(0);
		final Expr e = eLst.getExpr(v);
		assertEquals(expr, e);
		assertEquals(Var.alloc(alias.getSPARQLName()), v);
	}

	@Test
	public void testRegisterFunctionColumn() throws Exception {
		final ColumnName cName = new ColumnName("", "", "", "func");
		final QueryColumnInfo columnInfo = builder.registerFunctionColumn(
				cName, java.sql.Types.INTEGER);
		final QueryInfoSet infoSet = getInfoSet();
		final QueryItemCollection<QueryColumnInfo, Column, ColumnName> cols = infoSet
				.getColumns();
		assertEquals(1, cols.size());
		assertEquals(columnInfo, cols.get(0));
		final Column col = cols.get(0).getColumn();
		assertTrue(col instanceof FunctionColumn);
		final ColumnDef cd = col.getColumnDef();
		assertTrue(cd instanceof FunctionColumnDef);
		assertEquals(java.sql.Types.INTEGER, cd.getType());
		assertFalse(col.hasQuerySegments());
		assertNotNull(columnInfo.getTypeFilter());
		assertNotNull(columnInfo.getDataFilter());

	}

	@Test
	public void testRegisterFunction() throws Exception {
		final ColumnName cName = new ColumnName("", "", "", "func");
		final QueryColumnInfo columnInfo = builder.registerFunction(cName,
				java.sql.Types.INTEGER);
		final QueryInfoSet infoSet = getInfoSet();
		final QueryItemCollection<QueryColumnInfo, Column, ColumnName> cols = infoSet
				.getColumns();
		assertEquals(1, cols.size());
		assertEquals(columnInfo, cols.get(0));
		final Column col = cols.get(0).getColumn();
		assertTrue(col instanceof FunctionColumn);
		final ColumnDef cd = col.getColumnDef();
		assertTrue(cd instanceof FunctionColumnDef);
		assertEquals(java.sql.Types.INTEGER, cd.getType());
		assertFalse(col.hasQuerySegments());
		assertNotNull(columnInfo.getTypeFilter());
		assertNotNull(columnInfo.getDataFilter());

	}

	@Test
	public void testRegister() throws Exception {
		final Aggregator agg = new AggCount();
		final ExprAggregator expr = builder.register(agg,
				java.sql.Types.INTEGER, "alias");

		assertEquals(agg, expr.getAggregator());
		final QueryInfoSet infoSet = getInfoSet();

		final QueryItemCollection<QueryColumnInfo, Column, ColumnName> cols = infoSet
				.getColumns();
		assertEquals(1, cols.size());

		assertEquals("alias", cols.get(0).getVar().getName());
		final Column col = cols.get(0).getColumn();
		assertTrue(col instanceof FunctionColumn);
		final ColumnDef cd = col.getColumnDef();
		assertTrue(cd instanceof FunctionColumnDef);
		assertEquals(java.sql.Types.INTEGER, cd.getType());
		assertFalse(col.hasQuerySegments());
		assertNotNull(cols.get(0).getTypeFilter());
		assertNotNull(cols.get(0).getDataFilter());

	}

	@Test
	public void testGetCatalog() {
		final Catalog cat = builder.getCatalog();
		assertNotNull(cat);
		assertEquals(catalog, cat);
	}

	@Test
	public void testGetCatalogName() {
		final String s = builder.getCatalogName();
		assertNotNull(s);
		assertEquals(catName.getCatalog(), s);
	}

	@Test
	public void testGetCatalog_String() {
		final Catalog cat = builder.getCatalog(VirtualCatalog.NAME);
		assertNotNull(cat);
		assertEquals(vCat, cat);
	}

	@Test
	public void testGetColumn_ColumnName() {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		final QueryColumnInfo columnInfo = builder.getColumn(colName);
		assertNotNull(columnInfo);
		assertEquals(colName, columnInfo.getName());
		assertEquals(column, columnInfo.getColumn());
	}

	@Test
	public void testGetColumnCount() throws Exception {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
		builder.build();
		assertEquals(2, builder.getColumnCount());
	}

	@Test
	public void testGetColumn_Int() throws Exception {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
		builder.build();
		final QueryColumnInfo columnInfo = builder.getColumn(0);
		assertNotNull(columnInfo);
		assertEquals(colName.getGUID(), columnInfo.getGUID());
		assertEquals(column, columnInfo.getColumn());
	}

	@Test
	public void testGetColumnIndex() throws Exception {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
		builder.build();
		final int i = builder.getColumnIndex(colName.getSPARQLName());
		assertEquals(0, i);

	}

	@Test
	public void testGetDefaultSchemaName() {
		final String s = builder.getDefaultSchemaName();
		assertNotNull(s);
		assertEquals(sName.getShortName(), s);
	}

	@Test
	public void testGetDefaultSchema() {
		final Schema s = builder.getDefaultSchema();
		assertNotNull(s);
		assertEquals(schema, s);
	}

	@Test
	public void testGetDefaultTableName() {
		String s = builder.getDefaultTableName();
		assertNull(s);
		builder.addTable(table, tName, false);
		s = builder.getDefaultTableName();
		assertNotNull(s);
		assertEquals(table.getName().getShortName(), s);

		// create a second table
		final Table table2 = mock(Table.class);
		when(table2.getName()).thenReturn(sName.getTableName("table2"));
		when(table2.hasQuerySegments()).thenReturn(false);
		builder.addTable(table2, table2.getName(), false);

		s = builder.getDefaultTableName();
		assertNull(s);
	}

	@Test
	public void testGetSegments() {
		NameSegments s = builder.getSegments();
		assertNotNull(s);
		assertEquals(NameSegments.ALL, s);

		builder.addTable(table, tName, false);
		s = builder.getSegments();
		assertNotNull(s);
		assertEquals(NameSegments.ALL, s);

		builder.addColumnToQuery(colName, false);
		s = builder.getSegments();
		assertNotNull(s);
		assertEquals(NameSegments.ALL, s);

		builder.setSegmentCount();
		s = builder.getSegments();
		assertNotNull(s);
		assertEquals(NameSegments.FFFT, s);

	}

	@Test
	public void testGetTable() {
		assertNull(builder.getTable(tName));
		builder.addTable(table, tName, false);
		final QueryTableInfo tableInfo = builder.getTable(tName);
		assertNotNull(tableInfo);
		assertEquals(tName, tableInfo.getName());
		assertEquals(table, tableInfo.getTable());
	}

	@Test
	public void testSetAllColumns() throws Exception {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();

		final QueryInfoSet infoSet = getInfoSet();
		assertEquals(2, infoSet.getColumns().size());
		final Iterator<Column> cols = table.getColumns();
		while (cols.hasNext()) {
			assertNotNull(infoSet.getColumn(cols.next().getName()));
		}

	}

	@Test
	public void testSetDistinct() throws Exception {
		final Query query = getQuery();
		assertFalse(query.isDistinct());
		builder.setDistinct();
		assertTrue(query.isDistinct());
	}

	@Test
	public void testSetHaving() throws Exception {
		final Query query = getQuery();
		final Expr expr = new E_UUID();
		builder.setHaving(expr);
		assertNotNull(query.getHavingExprs());
		assertEquals(1, query.getHavingExprs().size());
		assertEquals(expr, query.getHavingExprs().get(0));
	}

	@Test
	public void testSetKey() throws Exception {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();

		final KeySegment segment = new KeySegment() {

			@Override
			public int compare(final Comparable<Object>[] o1, final Comparable<Object>[] o2) {
				return Utils.compare(getIdx(), isAscending(), o1, o2);
			}

			@Override
			public String getId() {
				return "TestKeySegment";
			}

			@Override
			public short getIdx() {
				return 0;
			}

			@Override
			public boolean isAscending() {
				return true;
			}
		};

		final Key key = new Key<KeySegment>() {

			@Override
			public int compare(final Comparable<Object>[] o1, final Comparable<Object>[] o2) {
				return Utils.compare(getSegments(), o1, o2);
			}

			@Override
			public String getId() {
				return "TestKey";
			}

			@Override
			public String getKeyName() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public List<KeySegment> getSegments() {
				return Arrays.asList(new KeySegment[] {
					segment
				});
			}

			@Override
			public boolean isUnique() {
				return true;
			}
		};

		builder.setKey(key);
		final Query query = getQuery();
		assertTrue(query.isDistinct());
		final List<SortCondition> lst = query.getOrderBy();
		assertEquals(1, lst.size());
		final SortCondition sc = lst.get(0);
		assertEquals(Query.ORDER_ASCENDING, sc.getDirection());
		assertEquals(new ExprVar(colName.getSPARQLName()), sc.getExpression());

	}

	@Test
	public void testSetLimit() throws Exception {
		final Query query = getQuery();
		assertEquals(Query.NOLIMIT, query.getLimit());
		builder.setLimit(5L);
		assertEquals(5L, query.getLimit());
		builder.setLimit(Query.NOLIMIT);
		assertEquals(Query.NOLIMIT, query.getLimit());
	}

	@Test
	public void testSetOffset() throws Exception {
		final Query query = getQuery();
		assertEquals(Query.NOLIMIT, query.getOffset());
		builder.setOffset(5L);
		assertEquals(5L, query.getOffset());
		builder.setOffset(Query.NOLIMIT);
		assertEquals(Query.NOLIMIT, query.getOffset());
	}

	@Test
	public void testSetOrderBy() throws Exception {
		builder.addTable(table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();

		final KeySegment segment = new KeySegment() {

			@Override
			public int compare(final Comparable<Object>[] o1, final Comparable<Object>[] o2) {
				return Utils.compare(getIdx(), isAscending(), o1, o2);
			}

			@Override
			public String getId() {
				return "TestKeySegment";
			}

			@Override
			public short getIdx() {
				return 0;
			}

			@Override
			public boolean isAscending() {
				return true;
			}
		};

		final Key key = new Key<KeySegment>() {

			@Override
			public int compare(final Comparable<Object>[] o1, final Comparable<Object>[] o2) {
				return Utils.compare(getSegments(), o1, o2);
			}

			@Override
			public String getId() {
				return "TestKey";
			}

			@Override
			public String getKeyName() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public List<KeySegment> getSegments() {
				return Arrays.asList(new KeySegment[] {
					segment
				});
			}

			@Override
			public boolean isUnique() {
				return true;
			}
		};

		builder.setOrderBy(key);
		final Query query = getQuery();
		assertFalse(query.isDistinct());
		final List<SortCondition> lst = query.getOrderBy();
		assertEquals(1, lst.size());
		final SortCondition sc = lst.get(0);
		assertEquals(Query.ORDER_ASCENDING, sc.getDirection());
		assertEquals(new ExprVar(colName.getSPARQLName()), sc.getExpression());

	}

	@Test
	public void testSetGUID() throws Exception {
		final QueryInfoSet infoSet = getInfoSet();
		assertFalse(infoSet.useGUID());
		builder.setUseGUID(true);
		assertTrue(infoSet.useGUID());
		builder.setUseGUID(false);
		assertFalse(infoSet.useGUID());

	}

	@Test
	public void testGetColumn_Node() throws Exception {
		final Var n = Var.alloc("testColumn");
		try {
			builder.getColumn(n);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {

		}
		final QueryTableInfo tableInfo = builder.addTable(table, tName, false);
		builder.addTableColumns(tableInfo);
		builder.setAllColumns();
		// reset for size
		final QueryColumnInfo columnInfo = builder.getColumn(n);
		assertEquals(colName.getSPARQLName(), columnInfo.getName()
				.getSPARQLName());
		assertEquals(column, columnInfo.getColumn());

	}

	@Test
	public void testGetTable_Node() throws Exception {
		final Var n = Var.alloc("testSchema" + NameUtils.SPARQL_DOT
				+ "testTable");
		assertNull(builder.getTable(n));

		final QueryTableInfo tableInfo = builder.addTable(table, tName, false);
		builder.addTableColumns(tableInfo);
		builder.setAllColumns();
		// reset for size
		final QueryTableInfo tableInfo2 = builder.getTable(n);
		assertEquals(tableInfo, tableInfo2);
		assertEquals(tName, tableInfo2.getName());
		assertEquals(table, tableInfo2.getTable());

	}

	@Test
	public void testGetResultColumns() throws SQLException {
		List<QueryColumnInfo> lst = builder.getResultColumns();
		assertNotNull(lst);
		assertTrue(lst.isEmpty());

		final QueryTableInfo tableInfo = builder.addTable(table, tName, false);
		builder.addTableColumns(tableInfo);
		builder.setAllColumns();
		// lst = builder.getResultColumns();
		// assertTrue( lst.isEmpty() );
		//
		// builder.build();
		lst = builder.getResultColumns();
		assertEquals(2, lst.size());

	}

}
