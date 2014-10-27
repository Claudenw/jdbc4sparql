// FIXME reimplement
//package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;
//
//import com.hp.hpl.jena.graph.NodeFactory;
//import com.hp.hpl.jena.sparql.expr.E_Equals;
//import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
//import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
//import com.hp.hpl.jena.sparql.expr.E_LogicalOr;
//import com.hp.hpl.jena.sparql.expr.E_NotEquals;
//import com.hp.hpl.jena.sparql.expr.E_Regex;
//import com.hp.hpl.jena.sparql.expr.Expr;
//import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import net.sf.jsqlparser.expression.Parenthesis;
//import net.sf.jsqlparser.expression.StringValue;
//import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
//import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
//import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
//import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
//import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
//import net.sf.jsqlparser.schema.Column;
//import net.sf.jsqlparser.schema.Table;
//
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.mockito.Matchers;
//import org.mockito.Mockito;
//import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
//
//public class SparqlExprVisitorTest
//{
//
//	private static Map<String, String> regexMap = new HashMap<String, String>();
//	private static Map<String, String> plainMap = new HashMap<String, String>();
//
//	@BeforeClass
//	public static void init()
//	{
//		SparqlExprVisitorTest.regexMap.put("%ab", "^(.+)\\Qab\\E$");
//		SparqlExprVisitorTest.regexMap.put("a%b", "^\\Qa\\E(.+)\\Qb\\E$");
//		SparqlExprVisitorTest.regexMap.put("ab%", "^\\Qab\\E(.+)$");
//		SparqlExprVisitorTest.regexMap.put("_ab", "^.\\Qab\\E$");
//		SparqlExprVisitorTest.regexMap.put("a_b", "^\\Qa\\E.\\Qb\\E$");
//		SparqlExprVisitorTest.regexMap.put("ab_", "^\\Qab\\E.$");
//		SparqlExprVisitorTest.regexMap.put("a.b_", "^\\Qa.b\\E.$");
//		SparqlExprVisitorTest.regexMap.put("a%b_", "^\\Qa\\E(.+)\\Qb\\E.$");
//		SparqlExprVisitorTest.regexMap.put("%", "^(.+)$");
//		SparqlExprVisitorTest.regexMap.put("_", "^.$");
//
//		
//		SparqlExprVisitorTest.plainMap.put("\\%ab", "%ab");
//		SparqlExprVisitorTest.plainMap.put("a\\%b", "a%b");
//		SparqlExprVisitorTest.plainMap.put("ab\\%", "ab%");
//		SparqlExprVisitorTest.plainMap.put("\\_ab", "_ab");
//		SparqlExprVisitorTest.plainMap.put("a\\_b", "a_b");
//		SparqlExprVisitorTest.plainMap.put("ab\\_", "ab_");
//		SparqlExprVisitorTest.plainMap.put("a.b", "a.b");
//		SparqlExprVisitorTest.plainMap.put("a([^a-z])b", "a([^a-z])b");
//		SparqlExprVisitorTest.plainMap.put("\\%", "%");
//		SparqlExprVisitorTest.plainMap.put("\\_", "_");
//	}
//
//	private SparqlExprVisitor visitor;
//	private SparqlQueryBuilder queryBuilder;
//	private Column col;
//
//	private Table tbl;
//
//	private void assertPlainEquals( final String msg, final int argPos,
//			final String value )
//	{
//		final Expr expr = visitor.getResult();
//		Assert.assertTrue("Error processing: " + msg, expr instanceof E_Equals);
//		final Expr arg = ((E_Equals) expr).getArg(argPos);
//		Assert.assertTrue("Error processing: " + msg,
//				arg instanceof NodeValueString);
//		Assert.assertEquals("Error processing: " + msg, value,
//				((NodeValueString) arg).getString());
//	}
//
//	private void assertPlainNotEquals( final String msg, final int argPos,
//			final String value )
//	{
//		final Expr expr = visitor.getResult();
//		Assert.assertTrue("Error processing: " + msg,
//				expr instanceof E_NotEquals);
//		final Expr arg = ((E_NotEquals) expr).getArg(argPos);
//		Assert.assertTrue("Error processing: " + msg,
//				arg instanceof NodeValueString);
//		Assert.assertEquals("Error processing: " + msg, value,
//				((NodeValueString) arg).getString());
//	}
//
//	private void assertRegex( final String msg, final Expr expr,
//			final String value )
//	{
//		Assert.assertTrue("Error processing: " + msg, expr instanceof E_Regex);
//		final E_Regex regex = (E_Regex) expr;
//		final Expr arg = regex.getArg(2);
//		Assert.assertTrue("Error processing: " + msg,
//				arg instanceof NodeValueString);
//		Assert.assertEquals("Error processing: " + msg, value,
//				((NodeValueString) arg).getString());
//	}
//
//	private void runEqualsLeft( final String initial )
//	{
//		final EqualsTo eq = new EqualsTo();
//		eq.setLeftExpression(new StringValue(String.format("'%s'", initial)));
//		eq.setRightExpression(col);
//		visitor.visit(eq);
//	}
//
//	private void runEqualsRight( final String initial )
//	{
//		final EqualsTo eq = new EqualsTo();
//		eq.setRightExpression(new StringValue(String.format("'%s'", initial)));
//		eq.setLeftExpression(col);
//		visitor.visit(eq);
//	}
//
//	private void runNotEqualsLeft( final String initial )
//	{
//		final NotEqualsTo neq = new NotEqualsTo();
//		neq.setLeftExpression(new StringValue(String.format("'%s'", initial)));
//		neq.setRightExpression(col);
//		visitor.visit(neq);
//	}
//
//	private void runNotEqualsRight( final String initial )
//	{
//		final NotEqualsTo neq = new NotEqualsTo();
//		neq.setRightExpression(new StringValue(String.format("'%s'", initial)));
//		neq.setLeftExpression(col);
//		visitor.visit(neq);
//	}
//
//	@Before
//	public void setup()
//	{
//		queryBuilder = Mockito.mock(SparqlQueryBuilder.class);
//		visitor = new SparqlExprVisitor(queryBuilder, false);
//		tbl = new Table("testSchema", "testTable");
//		col = new Column(tbl, "testCol");
//
//	}
//
//	@Test
//	public void testEqualsPlain() throws Exception
//	{
//		Mockito.when(
//				queryBuilder.addColumn(Matchers.anyString(),
//						Matchers.anyString(), Matchers.anyString(),
//						Matchers.anyBoolean())).thenReturn(
//				NodeFactory.createVariable("testVar"));
//
//		for (final String s : SparqlExprVisitorTest.plainMap.keySet())
//		{
//			System.out.println(s);
//			runEqualsLeft(s);
//			assertPlainEquals(s, 1, s);
//			runEqualsRight(s);
//			assertPlainEquals(s, 2, s);
//		}
//	}
//
//	@Test
//	public void testEqualsRegex() throws Exception
//	{
//		Mockito.when(
//				queryBuilder.addColumn(Matchers.anyString(),
//						Matchers.anyString(), Matchers.anyString(),
//						Matchers.anyBoolean())).thenReturn(
//				NodeFactory.createVariable("testVar"));
//
//		for (final String s : SparqlExprVisitorTest.regexMap.keySet())
//		{
//			runEqualsLeft(s);
//			assertPlainEquals(s, 1, s);
//			runEqualsRight(s);
//			assertPlainEquals(s, 2, s);
//		}
//	}
//
//	@Test
//	public void testLike() throws Exception
//	{
//		Mockito.when(
//				queryBuilder.addColumn(Matchers.anyString(),
//						Matchers.anyString(), Matchers.anyString(),
//						Matchers.anyBoolean())).thenReturn(
//				NodeFactory.createVariable("testVar"));
//		for (final String key : SparqlExprVisitorTest.regexMap.keySet())
//		{
//			final LikeExpression like = new LikeExpression();
//			like.setLeftExpression(col);
//			like.setRightExpression(new StringValue(String.format("'%s'", key)));
//			visitor.visit(like);
//			assertRegex(key, visitor.getResult(),
//					SparqlExprVisitorTest.regexMap.get(key));
//		}
//
//		for (final String key : SparqlExprVisitorTest.plainMap.keySet())
//		{
//			final LikeExpression like = new LikeExpression();
//			like.setLeftExpression(col);
//			like.setRightExpression(new StringValue(String.format("'%s'", key)));
//			visitor.visit(like);
//			assertPlainEquals( key,	2, SparqlExprVisitorTest.plainMap.get(key));
//		}
//	}
//
//	@Test
//	public void testNotEqualsPlain() throws Exception
//	{
//		Mockito.when(
//				queryBuilder.addColumn(Matchers.anyString(),
//						Matchers.anyString(), Matchers.anyString(),
//						Matchers.anyBoolean())).thenReturn(
//				NodeFactory.createVariable("testVar"));
//
//		for (final String s : SparqlExprVisitorTest.plainMap.keySet())
//		{
//			runNotEqualsLeft(s);
//			assertPlainNotEquals(s, 1, s);
//			runNotEqualsRight(s);
//			assertPlainNotEquals(s, 2, s);
//		}
//	}
//
//	@Test
//	public void testNotEqualsRegex() throws Exception
//	{
//		Mockito.when(
//				queryBuilder.addColumn(Matchers.anyString(),
//						Matchers.anyString(), Matchers.anyString(),
//						Matchers.anyBoolean())).thenReturn(
//				NodeFactory.createVariable("testVar"));
//
//		for (final String s : SparqlExprVisitorTest.regexMap.keySet())
//		{
//			runNotEqualsLeft(s);
//			assertPlainNotEquals(s, 1, s);
//			runNotEqualsRight(s);
//			assertPlainNotEquals(s, 2, s);
//		}
//	}
//	
//	@Test
//	public void testParenthesis() throws Exception
//	{
//		Mockito.when(
//				queryBuilder.addColumn(Matchers.anyString(),
//						Matchers.anyString(), Matchers.anyString(),
//						Matchers.anyBoolean())).thenReturn(
//				NodeFactory.createVariable("testVar"));
//		Parenthesis paren = new Parenthesis();
//			
//		EqualsTo eq = new EqualsTo();
//		eq.setLeftExpression(new StringValue(String.format("'%s'", "one")));
//		eq.setRightExpression(col);
//		
//		EqualsTo eq2 = new EqualsTo();
//		eq2.setLeftExpression(new StringValue(String.format("'%s'", "two")));
//		eq2.setRightExpression(col);
//		
//		OrExpression or = new OrExpression( eq, eq2);
//		
//		eq =  new EqualsTo();
//		eq.setLeftExpression(new StringValue(String.format("'%s'", "three")));
//		eq.setRightExpression(col);
//		
//		or = new OrExpression( or, eq );
//		paren.setExpression(or);
//		
//		eq =  new EqualsTo();
//		eq.setLeftExpression(new StringValue(String.format("'%s'", "four")));
//		eq.setRightExpression(col);
//		
//		AndExpression and = new AndExpression( eq, paren );
//		
//		eq =  new EqualsTo();
//		eq.setLeftExpression(new StringValue(String.format("'%s'", "five")));
//		eq.setRightExpression(col); 
//		and =  new AndExpression( and, eq );
//		System.out.println( and );
//		visitor.visit(and);
//		Expr expr = visitor.getResult();
//		Assert.assertTrue( expr instanceof E_LogicalAnd );
//		Assert.assertTrue( ((E_LogicalAnd)expr).getArg2() instanceof E_Equals);
//		Assert.assertEquals( "five", ((NodeValueString)((E_Equals)((E_LogicalAnd)expr).getArg2()).getArg1()).asString());
//		Assert.assertTrue( ((E_LogicalAnd)expr).getArg1() instanceof E_LogicalAnd);
//		E_LogicalAnd eAnd = (E_LogicalAnd)((E_LogicalAnd)expr).getArg1();
//		
//		Assert.assertTrue( eAnd.getArg1() instanceof E_Equals);
//		Assert.assertEquals( "four", ((NodeValueString)((E_Equals)eAnd.getArg1()).getArg1()).asString());
//		Assert.assertTrue( eAnd.getArg2() instanceof E_LogicalOr);
//		E_LogicalOr eOr = (E_LogicalOr) eAnd.getArg2();
//		
//		Assert.assertTrue( eOr.getArg2() instanceof E_Equals);
//		Assert.assertEquals( "three", ((NodeValueString)((E_Equals)eOr.getArg2()).getArg1()).asString());
//		Assert.assertTrue( eOr.getArg1() instanceof E_LogicalOr);
//		eOr = (E_LogicalOr) eOr.getArg1();
//		
//		Assert.assertTrue( eOr.getArg2() instanceof E_Equals);
//		Assert.assertEquals( "two", ((NodeValueString)((E_Equals)eOr.getArg2()).getArg1()).asString());
//		Assert.assertTrue( eOr.getArg1() instanceof E_Equals);
//		Assert.assertEquals( "one", ((NodeValueString)((E_Equals)eOr.getArg1()).getArg1()).asString());
//		
//	}
//}
