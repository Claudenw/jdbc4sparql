package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

import java.sql.SQLException;
import java.util.Stack;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import net.sf.jsqlparser.expression.Function;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.iface.CatalogName;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public class SystemFunctionHandlerTests
{
	private SystemFunctionHandler handler;
	private Stack<Expr> stack;
	private SparqlQueryBuilder builder;
	private Function func;
	private CatalogName catName;
	private RdfCatalog catalog;
	
	public SystemFunctionHandlerTests()
	{
		catName = new CatalogName( "catalogName");
		builder = mock(SparqlQueryBuilder.class);
		catalog = mock(RdfCatalog.class);
		when(builder.getCatalog()).thenReturn(catalog);
		when(catalog.getName()).thenReturn(catName);
	}
	
	@Before
	public void setup()
	{
		func = new Function();
		stack = new Stack<Expr>();
		handler = new SystemFunctionHandler(builder, stack);
	}
	
	@Test
	public void testCatalogFunction() throws SQLException
	{
		Expr expr = null;
		func.setName("catalog");
		assertTrue( handler.handle(func) );
		expr = stack.pop();
		assertTrue( expr instanceof NodeValueString );
		assertEquals( "catalogName", ((NodeValueString)expr).asString());
				
		func.setName("CATALOG");
		assertTrue( handler.handle(func) );
		expr = stack.pop();
		assertTrue( expr instanceof NodeValueString );
		assertEquals( "catalogName", ((NodeValueString)expr).asString());
		
		func.setName("CaTaLoG");
		assertTrue( handler.handle(func) );
		expr = stack.pop();
		assertTrue( expr instanceof NodeValueString );
		assertEquals( "catalogName", ((NodeValueString)expr).asString());
	}

	@Test
	public void testVersionFunction() throws SQLException
	{
		Expr expr = null;
		func.setName("version");
		assertTrue( handler.handle(func) );
		expr = stack.pop();
		assertTrue( expr instanceof NodeValueString );
		assertEquals( J4SDriver.getVersion(), ((NodeValueString)expr).asString());
				
		func.setName("VERSION");
		assertTrue( handler.handle(func) );
		expr = stack.pop();
		assertTrue( expr instanceof NodeValueString );
		assertEquals( J4SDriver.getVersion(), ((NodeValueString)expr).asString());
		
		func.setName("VerSion");
		assertTrue( handler.handle(func) );
		expr = stack.pop();
		assertTrue( expr instanceof NodeValueString );
		assertEquals( J4SDriver.getVersion(), ((NodeValueString)expr).asString());
	}

}
