package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.QueryInfoSet;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.expr.E_Add;
import org.apache.jena.sparql.expr.E_Bound;
import org.apache.jena.sparql.expr.E_Divide;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.E_LessThanOrEqual;
import org.apache.jena.sparql.expr.E_LogicalAnd;
import org.apache.jena.sparql.expr.E_LogicalNot;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_Multiply;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.E_OneOf;
import org.apache.jena.sparql.expr.E_Regex;
import org.apache.jena.sparql.expr.E_Subtract;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDT;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDouble;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;
import org.apache.jena.sparql.syntax.ElementGroup;

public class SparqlExprVisitorTest {

	private static Map<String, String> regexMap = new HashMap<String, String>();
	private static Map<String, String> plainMap = new HashMap<String, String>();
	private final static String SLASH = "\\";
	private final static String PATTERN = "[]^.?*+{}()|$";

	@BeforeClass
	public static void init() {
		SparqlExprVisitorTest.regexMap.put("%ab", "^(.+)ab$");
		SparqlExprVisitorTest.regexMap.put("a%b", "^a(.+)b$");
		SparqlExprVisitorTest.regexMap.put("ab%", "^ab(.+)$");
		SparqlExprVisitorTest.regexMap.put("_ab", "^.ab$");
		SparqlExprVisitorTest.regexMap.put("a_b", "^a.b$");
		SparqlExprVisitorTest.regexMap.put("a\\b_", "^a\\\\b.$");
		SparqlExprVisitorTest.regexMap.put("ab_", "^ab.$");
		SparqlExprVisitorTest.regexMap.put("a.b_", "^a\\.b.$");
		SparqlExprVisitorTest.regexMap.put("a%b_", "^a(.+)b.$");
		SparqlExprVisitorTest.regexMap.put("%", "^(.+)$");
		SparqlExprVisitorTest.regexMap.put("_", "^.$");

		SparqlExprVisitorTest.plainMap.put("\\%ab", "%ab");
		SparqlExprVisitorTest.plainMap.put("a\\%b", "a%b");
		SparqlExprVisitorTest.plainMap.put("ab\\%", "ab%");
		SparqlExprVisitorTest.plainMap.put("\\_ab", "_ab");
		SparqlExprVisitorTest.plainMap.put("a\\_b", "a_b");
		SparqlExprVisitorTest.plainMap.put("ab\\_", "ab_");
		SparqlExprVisitorTest.plainMap.put("a\\\\b", "a\\\\b");
		SparqlExprVisitorTest.plainMap.put("a\\b", "a\\b");
		SparqlExprVisitorTest.plainMap.put("a.b", "a.b");
		SparqlExprVisitorTest.plainMap.put("a([^a-z])b", "a([^a-z])b");
		SparqlExprVisitorTest.plainMap.put("\\%", "%");
		SparqlExprVisitorTest.plainMap.put("\\_", "_");

		for (int i = 0; i < PATTERN.length(); i++) {
			final String s = PATTERN.substring(i, i + 1);
			SparqlExprVisitorTest.plainMap.put("a" + s + "b", "a" + s + "b");
			SparqlExprVisitorTest.regexMap.put("a" + s + "b_", "^a" + SLASH + s
					+ "b.$");
		}
	}

	private SparqlExprVisitor visitor;
	private SparqlQueryBuilder queryBuilder;

	private QueryInfoSet queryInfoSet;
	private ElementGroup elementGroup;

	private QueryColumnInfo columnInfo;
	private ColumnName columnName;

	private QueryTableInfo tableInfo;
	private TableName tableName;

	private Column col;
	private Table tbl;

	private void assertPlainEquals(final String msg, final int argPos,
			final String value) {
		final Expr expr = visitor.getResult();
		Assert.assertTrue("Error processing: " + msg, expr instanceof E_Equals);
		final Expr arg = ((E_Equals) expr).getArg(argPos);
		Assert.assertTrue("Error processing: " + msg,
				arg instanceof NodeValueString);
		Assert.assertEquals("Error processing: " + msg, value,
				((NodeValueString) arg).getString());
	}

	private void assertPlainNotEquals(final String msg, final int argPos,
			final String value) {
		final Expr expr = visitor.getResult();
		Assert.assertTrue("Error processing: " + msg,
				expr instanceof E_NotEquals);
		final Expr arg = ((E_NotEquals) expr).getArg(argPos);
		Assert.assertTrue("Error processing: " + msg,
				arg instanceof NodeValueString);
		Assert.assertEquals("Error processing: " + msg, value,
				((NodeValueString) arg).getString());
	}

	private void assertRegex(final String msg, final Expr expr,
			final String value) {
		Assert.assertTrue("Error processing: " + msg, expr instanceof E_Regex);
		final E_Regex regex = (E_Regex) expr;
		final Expr arg = regex.getArg(2);
		Assert.assertTrue("Error processing: " + msg,
				arg instanceof NodeValueString);
		Assert.assertEquals("Error processing: " + msg, value,
				((NodeValueString) arg).getString());
	}

	private void runEqualsLeft(final String initial) {
		final EqualsTo eq = new EqualsTo();
		eq.setLeftExpression(new StringValue(String.format("'%s'", initial)));
		eq.setRightExpression(col);
		visitor.visit(eq);
	}

	private void runEqualsRight(final String initial) {
		final EqualsTo eq = new EqualsTo();
		eq.setRightExpression(new StringValue(String.format("'%s'", initial)));
		eq.setLeftExpression(col);
		visitor.visit(eq);
	}

	private void runNotEqualsLeft(final String initial) {
		final NotEqualsTo neq = new NotEqualsTo();
		neq.setLeftExpression(new StringValue(String.format("'%s'", initial)));
		neq.setRightExpression(col);
		visitor.visit(neq);
	}

	private void runNotEqualsRight(final String initial) {
		final NotEqualsTo neq = new NotEqualsTo();
		neq.setRightExpression(new StringValue(String.format("'%s'", initial)));
		neq.setLeftExpression(col);
		visitor.visit(neq);
	}

	@Before
	public void setup() throws SQLException {
		tbl = new Table("testSchema", "testTable");
		col = new Column(tbl, "testCol");

		columnName = new ColumnName("testCatalog", "testSchema", "testTable",
				"testCol");
		final org.xenei.jdbc4sparql.iface.Column column = mock(org.xenei.jdbc4sparql.iface.Column.class);
		when(column.getName()).thenReturn(columnName);

		columnInfo = new QueryColumnInfo(column);

		queryInfoSet = new QueryInfoSet();
		queryInfoSet.addColumn(columnInfo);

		tableName = columnName.getTableName();
		final org.xenei.jdbc4sparql.iface.Table table = mock(org.xenei.jdbc4sparql.iface.Table.class);
		when(table.getName()).thenReturn(tableName);

		elementGroup = new ElementGroup();

		tableInfo = new QueryTableInfo(queryInfoSet, elementGroup, table, false);

		queryBuilder = mock(SparqlQueryBuilder.class);
		when(
				queryBuilder.addColumn(Matchers.any(ColumnName.class),
						Matchers.anyBoolean())).thenReturn(columnInfo);
		when(queryBuilder.getCatalogName()).thenReturn("testCatalog");
		when(queryBuilder.getDefaultSchemaName()).thenReturn("testSchema");
		when(queryBuilder.getTable(Matchers.any(TableName.class))).thenReturn(
				tableInfo);

		visitor = new SparqlExprVisitor(queryBuilder, false, false);

	}

	@Test
	public void testEqualsPlain() throws Exception {

		when(
				queryBuilder.addColumn(Matchers.any(ColumnName.class),
						Matchers.anyBoolean())).thenReturn(columnInfo);

		for (final String s : SparqlExprVisitorTest.plainMap.keySet()) {
			runEqualsLeft(s);
			assertPlainEquals(s, 1, s);
			runEqualsRight(s);
			assertPlainEquals(s, 2, s);
		}
	}

	@Test
	public void testEqualsRegex() throws Exception {

		for (final String s : SparqlExprVisitorTest.regexMap.keySet()) {
			runEqualsLeft(s);
			assertPlainEquals(s, 1, s);
			runEqualsRight(s);
			assertPlainEquals(s, 2, s);
		}
	}

	@Test
	public void testVisitLikeExpression() throws Exception {

		for (final String key : SparqlExprVisitorTest.regexMap.keySet()) {
			final LikeExpression like = new LikeExpression();
			like.setLeftExpression(col);
			like.setRightExpression(new StringValue(String.format("'%s'", key)));
			visitor.visit(like);
			assertRegex(key, visitor.getResult(),
					SparqlExprVisitorTest.regexMap.get(key));
		}

		for (final String key : SparqlExprVisitorTest.plainMap.keySet()) {
			final LikeExpression like = new LikeExpression();
			like.setLeftExpression(col);
			like.setRightExpression(new StringValue(String.format("'%s'", key)));
			visitor.visit(like);
			assertPlainEquals(key, 2, SparqlExprVisitorTest.plainMap.get(key));
		}
	}

	// @Test
	// public void testLikeExpression() throws Exception {
	// // method to test individual like expressions during development
	//
	// final LikeExpression like = new LikeExpression();
	// like.setLeftExpression(col);
	// like.setRightExpression(new StringValue("'a\\b_'"));
	// visitor.visit(like);
	// assertPlainEquals("test", 2, "^a\\b.$");
	//
	// }

	@Test
	public void testNotEqualsPlain() throws Exception {

		for (final String s : SparqlExprVisitorTest.plainMap.keySet()) {
			runNotEqualsLeft(s);
			assertPlainNotEquals(s, 1, s);
			runNotEqualsRight(s);
			assertPlainNotEquals(s, 2, s);
		}
	}

	@Test
	public void testNotEqualsRegex() throws Exception {

		for (final String s : SparqlExprVisitorTest.regexMap.keySet()) {
			runNotEqualsLeft(s);
			assertPlainNotEquals(s, 1, s);
			runNotEqualsRight(s);
			assertPlainNotEquals(s, 2, s);
		}
	}

	@Test
	public void testVisitNullValue() {
		final NullValue nullValue = new NullValue();
		try {
			visitor.visit(nullValue);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitFunctionMax() throws Exception {
		final Function func = new Function();
		final TestableExpressionList lst = new TestableExpressionList(1);
		when(lst.getExpr(0).getVarName()).thenReturn("foo");
		func.setName("MAX");
		func.setParameters(lst);

		final ExprAggregator expr = mock(ExprAggregator.class);
		final ExprVar var = new ExprVar("foo");
		when(expr.getAggVar()).thenReturn(var);
		when(
				queryBuilder.register(any(Aggregator.class), anyInt(),
						anyString())).thenReturn(expr);
		when(queryBuilder.getColumn(any(ColumnName.class))).thenReturn(
				columnInfo);
		visitor.visit(func);
	}

	@Test
	public void testVisitInverseExpression() {
		final InverseExpression expr = new InverseExpression();
		try {
			visitor.visit(expr);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitJDBCParameter() {
		final JdbcParameter param = new JdbcParameter();
		try {
			visitor.visit(param);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitDoubleValue() {
		final DoubleValue val = new DoubleValue("5.3");
		visitor.visit(val);
		final Expr expr = visitor.getResult();
		assertTrue(expr instanceof NodeValueDouble);
		assertEquals(new NodeValueDouble(5.3), expr);
	}

	@Test
	public void testVisitLongValue() {
		final LongValue val = new LongValue("5");
		visitor.visit(val);
		final Expr expr = visitor.getResult();
		assertTrue(expr instanceof NodeValueInteger);
		assertEquals(new NodeValueInteger(5), expr);
	}

	@Test
	public void testVisitDateValue() {
		final DateValue val = new DateValue("'2001-05-25'");
		visitor.visit(val);
		final Expr expr = visitor.getResult();
		assertTrue(expr instanceof NodeValueDT);
		final Node n = NodeFactory.createLiteral("2001-05-25",
				XSDDatatype.XSDdate);
		assertEquals(new NodeValueDT("2001-05-25", n), expr);
	}

	@Test
	public void testVisitTimeValue() {
		final TimeValue val = new TimeValue("'20:34:12'");
		visitor.visit(val);
		final Expr expr = visitor.getResult();
		assertTrue(expr instanceof NodeValueDT);
		final Node n = NodeFactory.createLiteral("20:34:12",
				XSDDatatype.XSDtime);
		assertEquals(new NodeValueDT("20:34:12", n), expr);
	}

	@Test
	public void testVisitTimestampValue() {
		TimestampValue val = new TimestampValue("'2001-05-25 20:34:12'");
		visitor.visit(val);
		Expr expr = visitor.getResult();
		assertTrue(expr instanceof NodeValueDT);
		Node n = NodeFactory.createLiteral("2001-05-25T20:34:12.0",
				XSDDatatype.XSDdateTime);
		assertEquals(new NodeValueDT("2001-05-25T20:34:12.0", n), expr);

		val = new TimestampValue("'2001-05-25 20:34:12.5'");
		visitor.visit(val);
		expr = visitor.getResult();
		assertTrue(expr instanceof NodeValueDT);
		n = NodeFactory.createLiteral("2001-05-25T20:34:12.5",
				XSDDatatype.XSDdateTime);
		assertEquals(new NodeValueDT("2001-05-25T20:34:12.5", n), expr);
	}

	@Test
	public void testVisitParenthesis() {
		Parenthesis paren = new Parenthesis();
		final Expression expression = mock(Expression.class);
		paren.setExpression(expression);
		visitor.visit(paren);
		verify(expression).accept(visitor);

		// verify that the parenthesis change the ordering.
		paren = new Parenthesis();

		EqualsTo eq = new EqualsTo();
		eq.setLeftExpression(new StringValue(String.format("'%s'", "one")));
		eq.setRightExpression(col);

		final EqualsTo eq2 = new EqualsTo();
		eq2.setLeftExpression(new StringValue(String.format("'%s'", "two")));
		eq2.setRightExpression(col);

		OrExpression or = new OrExpression(eq, eq2);

		eq = new EqualsTo();
		eq.setLeftExpression(new StringValue(String.format("'%s'", "three")));
		eq.setRightExpression(col);

		or = new OrExpression(or, eq);
		paren.setExpression(or);

		eq = new EqualsTo();
		eq.setLeftExpression(new StringValue(String.format("'%s'", "four")));
		eq.setRightExpression(col);

		AndExpression and = new AndExpression(eq, paren);

		eq = new EqualsTo();
		eq.setLeftExpression(new StringValue(String.format("'%s'", "five")));
		eq.setRightExpression(col);
		and = new AndExpression(and, eq);

		visitor.visit(and);
		final Expr expr = visitor.getResult();
		Assert.assertTrue(expr instanceof E_LogicalAnd);
		Assert.assertTrue(((E_LogicalAnd) expr).getArg2() instanceof E_Equals);
		Assert.assertEquals("five",
				((NodeValueString) ((E_Equals) ((E_LogicalAnd) expr).getArg2())
						.getArg1()).asString());
		Assert.assertTrue(((E_LogicalAnd) expr).getArg1() instanceof E_LogicalAnd);
		final E_LogicalAnd eAnd = (E_LogicalAnd) ((E_LogicalAnd) expr)
				.getArg1();

		Assert.assertTrue(eAnd.getArg1() instanceof E_Equals);
		Assert.assertEquals("four", ((NodeValueString) ((E_Equals) eAnd
				.getArg1()).getArg1()).asString());
		Assert.assertTrue(eAnd.getArg2() instanceof E_LogicalOr);
		E_LogicalOr eOr = (E_LogicalOr) eAnd.getArg2();

		Assert.assertTrue(eOr.getArg2() instanceof E_Equals);
		Assert.assertEquals("three", ((NodeValueString) ((E_Equals) eOr
				.getArg2()).getArg1()).asString());
		Assert.assertTrue(eOr.getArg1() instanceof E_LogicalOr);
		eOr = (E_LogicalOr) eOr.getArg1();

		Assert.assertTrue(eOr.getArg2() instanceof E_Equals);
		Assert.assertEquals("two",
				((NodeValueString) ((E_Equals) eOr.getArg2()).getArg1())
						.asString());
		Assert.assertTrue(eOr.getArg1() instanceof E_Equals);
		Assert.assertEquals("one",
				((NodeValueString) ((E_Equals) eOr.getArg1()).getArg1())
						.asString());

	}

	@Test
	public void testVisitStringValue() {
		final StringValue val = new StringValue("'Hello there'");
		visitor.visit(val);
		final Expr expr = visitor.getResult();
		assertTrue(expr instanceof NodeValueString);
		assertEquals(new NodeValueString("Hello there"), expr);
	}

	private void visitBinaryExpression(final BinaryExpression expr,
			final Class<? extends ExprFunction2> clazz) throws Exception {

		final TestableExpression expression1 = new TestableExpression();
		final TestableExpression expression2 = new TestableExpression();
		expr.setLeftExpression(expression1);
		expr.setRightExpression(expression2);
		expr.accept(visitor);
		final Expr result = visitor.getResult();
		assertEquals(clazz, result.getClass());
		assertEquals(expression1.getExpr(), ((ExprFunction2) result).getArg1());
		assertEquals(expression2.getExpr(), ((ExprFunction2) result).getArg2());
	}

	@Test
	public void testVisitAddition() throws Exception {
		visitBinaryExpression(new Addition(), E_Add.class);
	}

	@Test
	public void testVisitDivision() throws Exception {
		visitBinaryExpression(new Division(), E_Divide.class);
	}

	@Test
	public void testVisitMultiplication() throws Exception {
		visitBinaryExpression(new Multiplication(), E_Multiply.class);
	}

	@Test
	public void testVisitSubtraction() throws Exception {
		visitBinaryExpression(new Subtraction(), E_Subtract.class);
	}

	@Test
	public void testVisitAndExpression() throws Exception {
		visitBinaryExpression(new AndExpression(null, null), E_LogicalAnd.class);
	}

	@Test
	public void testVisitOrExpression() throws Exception {
		visitBinaryExpression(new OrExpression(null, null), E_LogicalOr.class);
	}

	@Test
	public void testVisitBetween() throws Exception {
		final Between var = new Between();
		final TestableExpression expr1 = new TestableExpression();
		final TestableExpression expr2 = new TestableExpression();
		final TestableExpression expr3 = new TestableExpression();
		var.setBetweenExpressionStart(expr1);
		var.setBetweenExpressionEnd(expr2);
		var.setLeftExpression(expr3);
		visitor.visit(var);
		final Expr result = visitor.getResult();

		assertTrue(result instanceof E_LogicalAnd);
		final E_LogicalAnd r = (E_LogicalAnd) result;

		assertTrue(r.getArg1() instanceof E_LessThanOrEqual);
		final E_LessThanOrEqual le = (E_LessThanOrEqual) r.getArg1();
		assertEquals(expr1.getExpr(), le.getArg1());
		assertEquals(expr3.getExpr(), le.getArg2());

		assertTrue(r.getArg2() instanceof E_GreaterThanOrEqual);
		final E_GreaterThanOrEqual ge = (E_GreaterThanOrEqual) r.getArg2();
		assertEquals(expr3.getExpr(), ge.getArg1());
		assertEquals(expr2.getExpr(), ge.getArg2());

	}

	@Test
	public void testVisitEqualsTo() throws Exception {
		visitBinaryExpression(new EqualsTo(), E_Equals.class);
	}

	@Test
	public void testVisitGreaterThan() throws Exception {
		visitBinaryExpression(new GreaterThan(), E_GreaterThan.class);
	}

	@Test
	public void testVisitGreaterThanEquals() throws Exception {
		visitBinaryExpression(new GreaterThanEquals(),
				E_GreaterThanOrEqual.class);
	}

	@Test
	public void testVisitInExpression() throws Exception {
		final TestableExpression expr = new TestableExpression();
		final TestableExpressionList list = new TestableExpressionList(2);
		final InExpression var = new InExpression(expr, list);
		visitor.visit(var);
		final Expr result = visitor.getResult();
		assertTrue(result instanceof E_OneOf);
		final E_OneOf oneOf = (E_OneOf) result;
		assertEquals(expr.getExpr(), oneOf.getLHS());
		assertEquals(list.getExpr(0), oneOf.getRHS().get(0));
		assertEquals(list.getExpr(1), oneOf.getRHS().get(1));
	}

	@Test
	public void testVisitIsNullExpression() throws Exception {
		final IsNullExpression isNull = new IsNullExpression();
		final TestableExpression expression = new TestableExpression();
		isNull.setLeftExpression(expression);
		visitor.visit(isNull);
		Expr result = visitor.getResult();
		assertTrue(result instanceof E_LogicalNot);
		result = ((E_LogicalNot) result).getArg();
		assertTrue(result instanceof E_Bound);
		E_Bound bound = (E_Bound) result;
		assertEquals(expression.getExpr(), bound.getArg());

		isNull.setNot(true);
		visitor.visit(isNull);
		result = visitor.getResult();
		assertTrue(result instanceof E_Bound);
		bound = (E_Bound) result;
		assertEquals(expression.getExpr(), bound.getArg());
	}

	@Test
	public void testVisitMinorThan() throws Exception {
		visitBinaryExpression(new MinorThan(), E_LessThan.class);
	}

	@Test
	public void testVisitMinorThanEquals() throws Exception {
		visitBinaryExpression(new MinorThanEquals(), E_LessThanOrEqual.class);
	}

	@Test
	public void testVisitNotEqualsTo() throws Exception {
		visitBinaryExpression(new NotEqualsTo(), E_NotEquals.class);
	}

	@Test
	public void testVisitColumn() {
		visitor.visit(col);
		final Expr expr = visitor.getResult();
		assert (expr instanceof ExprColumn);
		final ExprColumn exprCol = (ExprColumn) expr;
		assertEquals(columnInfo, exprCol.getColumnInfo());
		assertTrue(queryInfoSet.containsColumn(columnInfo.getName()));
	}

	@Test
	public void testVisitSubSelect() {
		final SubSelect op = new SubSelect();
		try {
			visitor.visit(op);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitCaseExpression() {
		final CaseExpression op = new CaseExpression();
		try {
			visitor.visit(op);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitWhenClause() {
		final WhenClause op = new WhenClause();
		try {
			visitor.visit(op);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitExistsExpression() {
		final ExistsExpression exists = new ExistsExpression();
		try {
			visitor.visit(exists);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitAllComparisonExpression() {
		final AllComparisonExpression param = new AllComparisonExpression(
				mock(SubSelect.class));
		try {
			visitor.visit(param);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitAnyComparisonExpression() {
		final AnyComparisonExpression param = new AnyComparisonExpression(
				mock(SubSelect.class));
		try {
			visitor.visit(param);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitConcat() {
		final Concat concat = new Concat();
		try {
			visitor.visit(concat);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitMatches() {
		final Matches matches = new Matches();
		try {
			visitor.visit(matches);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitBitwiseAnd() {
		final BitwiseAnd op = new BitwiseAnd();
		try {
			visitor.visit(op);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitBitwiseOr() {
		final BitwiseOr op = new BitwiseOr();
		try {
			visitor.visit(op);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	@Test
	public void testVisitBitwiseXor() {
		final BitwiseXor op = new BitwiseXor();
		try {
			visitor.visit(op);
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
		}
	}

	private class TestableExpression implements Expression {

		private final Expr expr;
		private final Field f;

		public TestableExpression() throws Exception {
			this.expr = mock(Expr.class);
			this.f = SparqlExprVisitor.class.getDeclaredField("stack");
			f.setAccessible(true);
		}

		@Override
		public void accept(final ExpressionVisitor expressionVisitor) {
			try {
				final Stack<Expr> stack = (Stack<Expr>) f
						.get(expressionVisitor);
				stack.push(expr);
			} catch (final IllegalAccessException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}

		public Expr getExpr() {
			return expr;
		}

	};

	private class TestableExpressionList extends ExpressionList {

		public TestableExpressionList(final int size) throws Exception {
			final List<TestableExpression> lst = new ArrayList<TestableExpression>();
			for (int i = 0; i < size; i++) {
				lst.add(new TestableExpression());
			}
			this.setExpressions(lst);
		}

		@Override
		public void accept(final ItemsListVisitor itemsListVisitor) {
			itemsListVisitor.visit(this);
		}

		public Expr getExpr(final int idx) {
			return ((TestableExpression) this.getExpressions().get(idx))
					.getExpr();
		}

	}
}
