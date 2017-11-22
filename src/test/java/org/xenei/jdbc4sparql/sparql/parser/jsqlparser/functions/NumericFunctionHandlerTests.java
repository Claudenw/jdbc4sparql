package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.SchemaName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.sparql.QueryInfoSet;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.AliasInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfo;

import org.apache.jena.sparql.expr.E_NumAbs;
import org.apache.jena.sparql.expr.E_NumCeiling;
import org.apache.jena.sparql.expr.E_NumFloor;
import org.apache.jena.sparql.expr.E_NumRound;
import org.apache.jena.sparql.expr.E_Random;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggMax;
import org.apache.jena.sparql.expr.aggregate.AggMin;
import org.apache.jena.sparql.expr.aggregate.AggSum;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDouble;

public class NumericFunctionHandlerTests {
	private NumericFunctionHandler handler;
	private List<Expression> lst2;
	private ExpressionList expressionList;

	private SparqlQueryBuilder queryBuilder;

	private QueryInfoSet queryInfoSet;

	private QueryColumnInfo columnInfo;
	private ColumnName columnName;

	private TableName tableName;

	private net.sf.jsqlparser.schema.Column col;
	private net.sf.jsqlparser.schema.Table tbl;

	private Function func;

	private Map<String, Catalog> catalogs;
	private Catalog catalog;
	private CatalogName catalogName;

	private Schema schema;
	private SchemaName schemaName;

	private AliasInfo alias;

	public NumericFunctionHandlerTests() {
	}

	@Before
	public void setup() throws Exception {

		tbl = new net.sf.jsqlparser.schema.Table("testSchema", "testTable");
		col = new net.sf.jsqlparser.schema.Column(tbl, "testCol");

		columnName = new ColumnName("testCatalog", "testSchema", "testTable",
				"testCol");
		final org.xenei.jdbc4sparql.iface.Column column = mock(org.xenei.jdbc4sparql.iface.Column.class);
		when(column.getName()).thenReturn(columnName);

		columnInfo = new QueryColumnInfo(column);

		queryInfoSet = new QueryInfoSet();
		queryInfoSet.addColumn(columnInfo);

		tableName = columnName.getTableName();
		org.xenei.jdbc4sparql.iface.Table table = mock(org.xenei.jdbc4sparql.iface.Table.class);
		when(table.getName()).thenReturn(tableName);

		catalogs = new HashMap<String, Catalog>();
		final SparqlParser parser = mock(SparqlParser.class);

		catalog = mock(Catalog.class);
		catalogName = new CatalogName("testCatalog");
		when(catalog.getName()).thenReturn(catalogName);
		when(catalog.getShortName()).thenReturn("testCatalog");
		catalogs.put("testCatalog", catalog);
		catalogs.put(VirtualCatalog.NAME, new VirtualCatalog());

		schema = mock(Schema.class);
		schemaName = catalogName.getSchemaName("testSchema");
		when(schema.getName()).thenReturn(schemaName);

		table = mock(Table.class);
		final TableName tableName = schemaName.getTableName("testTable");
		when(table.getColumn(eq("testCol"))).thenReturn(column);

		queryBuilder = new SparqlQueryBuilder(catalogs, parser, catalog, schema);
		queryBuilder.addTable(table, tableName, false);

		final SparqlExprVisitor visitor = new SparqlExprVisitor(queryBuilder,
				false, false);
		alias = visitor.new AliasInfo("Alias", false);
		func = new Function();
		handler = new NumericFunctionHandler(queryBuilder);
		lst2 = new ArrayList<Expression>();
		expressionList = new ExpressionList(lst2);
	}

	@Test
	public void testAbsFunction() throws SQLException {
		func.setName("abs");
		lst2.add(new DoubleValue("2.5"));
		func.setParameters(expressionList);

		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_NumAbs);
		final E_NumAbs expr2 = (E_NumAbs) expr;
		final List<Expr> lst = expr2.getArgs();
		assertEquals(1, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueDouble);
		final NodeValueDouble n = (NodeValueDouble) lst.get(0);
		assertEquals(Double.valueOf(2.5), Double.valueOf(n.getDouble()));
	}

	@Test
	public void testCeilFunction() throws SQLException {
		func.setName("ceil");
		lst2.add(new DoubleValue("2.5"));
		func.setParameters(expressionList);

		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_NumCeiling);
		final E_NumCeiling expr2 = (E_NumCeiling) expr;
		final List<Expr> lst = expr2.getArgs();
		assertEquals(1, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueDouble);
		final NodeValueDouble n = (NodeValueDouble) lst.get(0);
		assertEquals(Double.valueOf(2.5), Double.valueOf(n.getDouble()));
	}

	@Test
	public void testCountFunction() throws SQLException {
		func.setName("count");
		lst2.add(col);
		func.setParameters(expressionList);

		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof ExprAggregator);
		final ExprAggregator agg = (ExprAggregator) expr;
		assertTrue(agg.getAggregator() instanceof AggCountVar);
		final Aggregator agg2 = agg.getAggregator();
		final ExprList exprLst = agg2.getExprList();
		assertEquals( 1, exprLst.size() );
		Expr expr2 = exprLst.get(0);
		assertTrue(expr2 instanceof ExprColumn);
		assertEquals(columnName.getGUID(), ((ExprColumn) expr2).getColumnInfo().getName().getGUID());
	}

	@Test
	public void testFloorFunction() throws SQLException {
		func.setName("floor");
		lst2.add(new DoubleValue("2.5"));
		func.setParameters(expressionList);

		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_NumFloor);
		final E_NumFloor expr2 = (E_NumFloor) expr;
		final List<Expr> lst = expr2.getArgs();
		assertEquals(1, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueDouble);
		final NodeValueDouble n = (NodeValueDouble) lst.get(0);
		assertEquals(Double.valueOf(2.5), Double.valueOf(n.getDouble()));
	}

	@Test
	public void testMaxFunction() throws SQLException {
		func.setName("max");
		lst2.add(col);
		func.setParameters(expressionList);
		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof ExprAggregator);
		final ExprAggregator agg = (ExprAggregator) expr;
		assertTrue(agg.getAggregator() instanceof AggMax);
		final Aggregator agg2 = agg.getAggregator();
		final ExprList exprLst = agg2.getExprList();
		assertEquals( 1, exprLst.size() );
		Expr expr2 = exprLst.get(0);
		assertTrue(expr2 instanceof ExprColumn);
		assertEquals(columnName.getGUID(), ((ExprColumn) expr2).getColumnInfo().getName().getGUID());
	}

	@Test
	public void testMinFunction() throws SQLException {
		func.setName("min");
		lst2.add(col);
		func.setParameters(expressionList);

		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof ExprAggregator);
		final ExprAggregator agg = (ExprAggregator) expr;
		assertTrue(agg.getAggregator() instanceof AggMin);
		final Aggregator agg2 = agg.getAggregator();
		final ExprList exprLst = agg2.getExprList();
		assertEquals( 1, exprLst.size() );
		Expr expr2 = exprLst.get(0);
		assertTrue(expr2 instanceof ExprColumn);
		assertEquals(columnName.getGUID(), ((ExprColumn) expr2).getColumnInfo().getName().getGUID());
	}

	@Test
	public void testRandFunction() throws SQLException {

		func.setName("rand");
		// func.setParameters(expressionList);
		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_Random);
		final E_Random expr2 = (E_Random) expr;
		final List<Expr> lst = expr2.getArgs();
		assertEquals(0, lst.size());
	}

	@Test
	public void testRoundFunction() throws SQLException {
		func.setName("round");
		lst2.add(new DoubleValue("2.5"));
		func.setParameters(expressionList);

		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_NumRound);
		final E_NumRound expr2 = (E_NumRound) expr;
		final List<Expr> lst = expr2.getArgs();
		assertEquals(1, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueDouble);
		final NodeValueDouble n = (NodeValueDouble) lst.get(0);
		assertEquals(Double.valueOf(2.5), Double.valueOf(n.getDouble()));
	}

	@Test
	public void testSumFunction() throws SQLException {
		func.setName("sum");
		lst2.add(col);
		func.setParameters(expressionList);

		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof ExprAggregator);
		final ExprAggregator agg = (ExprAggregator) expr;
		assertTrue(agg.getAggregator() instanceof AggSum);
		final Aggregator agg2 = agg.getAggregator();
		final ExprList exprLst = agg2.getExprList();
		assertEquals( 1, exprLst.size() );
		Expr expr2 = exprLst.get(0);
		assertTrue(expr2 instanceof ExprColumn);
		assertEquals(columnName.getGUID(), ((ExprColumn) expr2).getColumnInfo().getName().getGUID());
	}
}
