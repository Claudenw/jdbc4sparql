package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.AliasInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfo;

import org.apache.jena.sparql.expr.E_StrLength;
import org.apache.jena.sparql.expr.E_StrLowerCase;
import org.apache.jena.sparql.expr.E_StrReplace;
import org.apache.jena.sparql.expr.E_StrSubstring;
import org.apache.jena.sparql.expr.E_StrUpperCase;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;

public class StringFunctionHandlerTests {
	private StringFunctionHandler handler;
	private final SparqlQueryBuilder builder;
	private Function func;
	private final CatalogName catName;
	private final RdfCatalog catalog;
	private final ColumnDef colDef;
	private StringValue stringValue;
	private ExpressionList expressionList;
	private String text;
	private List<Expression> lst2;
	private AliasInfo alias;

	public StringFunctionHandlerTests() {
		final QueryColumnInfo colInfo = mock(QueryColumnInfo.class);
		catName = new CatalogName("catalogName");
		builder = mock(SparqlQueryBuilder.class);
		catalog = mock(RdfCatalog.class);
		final Column column = mock(Column.class);
		colDef = mock(ColumnDef.class);
		when(builder.getCatalog()).thenReturn(catalog);
		when(builder.getCatalogName()).thenReturn( catName.getCatalog() );
		when(catalog.getName()).thenReturn(catName);
		when(builder.getColumn((ColumnName) any())).thenReturn(colInfo);
		when(colInfo.getColumn()).thenReturn(column);
		when(column.getColumnDef()).thenReturn(colDef);
	}

	@Before
	public void setup() {
		text = "AString";
		func = new Function();
		handler = new StringFunctionHandler(builder);
		stringValue = new StringValue(String.format("'%s'", text));
		lst2 = new ArrayList<Expression>();
		lst2.add(stringValue);
		expressionList = new ExpressionList(lst2);
		final SparqlExprVisitor visitor = new SparqlExprVisitor(builder, false,
				false);
		alias = visitor.new AliasInfo("Alias", false, java.sql.Types.VARCHAR);
	}

	@Test
	public void testLCaseFunction() throws SQLException {
		func.setName("LCASE");
		func.setParameters(expressionList);
		ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrLowerCase);
		E_StrLowerCase expr2 = (E_StrLowerCase) expr;
		assertTrue(expr2.getArg() instanceof NodeValueString);
		NodeValueString n = (NodeValueString) expr2.getArg();
		assertEquals(text, n.getString());

		func.setName("lower");
		func.setParameters(expressionList);
		exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrLowerCase);
		expr2 = (E_StrLowerCase) expr;
		assertTrue(expr2.getArg() instanceof NodeValueString);
		n = (NodeValueString) expr2.getArg();
		assertEquals(text, n.getString());
	}

	@Test
	public void testLengthFunction() throws SQLException {
		func.setName("length");
		func.setParameters(expressionList);
		ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrLength);
		E_StrLength expr2 = (E_StrLength) expr;
		List<Expr> lst = expr2.getArgs();
		assertEquals(1, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueString);
		NodeValueString n = (NodeValueString) lst.get(0);
		assertEquals(text, n.getString());

		func.setName("len");
		func.setParameters(expressionList);
		exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrLength);
		expr2 = (E_StrLength) expr;
		lst = expr2.getArgs();
		assertEquals(1, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueString);
		n = (NodeValueString) lst.get(0);
		assertEquals(text, n.getString());
	}

	@Test
	public void testReplaceFunction() throws SQLException {
		func.setName("replace");
		lst2.add(new StringValue("'A'"));
		lst2.add(new StringValue("'The '"));
		func.setParameters(expressionList);
		final ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		final Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrReplace);
		final E_StrReplace expr2 = (E_StrReplace) expr;

		final List<Expr> lst = expr2.getArgs();
		assertEquals(3, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueString);
		NodeValueString n = (NodeValueString) lst.get(0);
		assertEquals(text, n.getString());

		assertTrue(lst.get(1) instanceof NodeValueString);
		n = (NodeValueString) lst.get(1);
		assertEquals("A", n.getString());

		assertTrue(lst.get(2) instanceof NodeValueString);
		n = (NodeValueString) lst.get(2);
		assertEquals("The ", n.getString());

	}

	@Test
	public void testSubstringFunction() throws SQLException {
		func.setName("substring");
		lst2.add(new LongValue("2"));
		func.setParameters(expressionList);

		ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrSubstring);
		E_StrSubstring expr2 = (E_StrSubstring) expr;
		List<Expr> lst = expr2.getArgs();
		assertEquals(2, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueString);
		NodeValueString n = (NodeValueString) lst.get(0);
		assertEquals(text, n.getString());

		assertTrue(lst.get(1) instanceof NodeValueInteger);
		NodeValueInteger i = (NodeValueInteger) lst.get(1);
		assertEquals(2, i.getInteger().intValue());

		lst2.add(new LongValue("5"));
		func.setParameters(expressionList);
		exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrSubstring);
		expr2 = (E_StrSubstring) expr;
		lst = expr2.getArgs();
		assertEquals(3, lst.size());

		assertTrue(lst.get(0) instanceof NodeValueString);
		n = (NodeValueString) lst.get(0);
		assertEquals(text, n.getString());

		assertTrue(lst.get(1) instanceof NodeValueInteger);
		i = (NodeValueInteger) lst.get(1);
		assertEquals(2, i.getInteger().intValue());

		assertTrue(lst.get(2) instanceof NodeValueInteger);
		i = (NodeValueInteger) lst.get(2);
		assertEquals(5, i.getInteger().intValue());

	}

	@Test
	public void testUCaseFunction() throws SQLException {
		func.setName("UCASE");
		func.setParameters(expressionList);
		ExprInfo exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		Expr expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrUpperCase);
		E_StrUpperCase expr2 = (E_StrUpperCase) expr;
		assertTrue(expr2.getArg() instanceof NodeValueString);
		NodeValueString n = (NodeValueString) expr2.getArg();
		assertEquals(text, n.getString());

		func.setName("upper");
		func.setParameters(expressionList);
		exprInfo = (ExprInfo) handler.handle(func, alias);
		assertEquals(alias.getAlias(), exprInfo.getName().getShortName());
		expr = exprInfo.getExpr();
		assertTrue(expr instanceof E_StrUpperCase);
		expr2 = (E_StrUpperCase) expr;
		assertTrue(expr2.getArg() instanceof NodeValueString);
		n = (NodeValueString) expr2.getArg();
		assertEquals(text, n.getString());
	}
}
