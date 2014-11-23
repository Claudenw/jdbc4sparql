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
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

public class SystemFunctionHandlerTests {
	private SystemFunctionHandler handler;
	private Stack<Expr> stack;
	private SparqlQueryBuilder builder;
	private Function func;
	private CatalogName catName;
	private RdfCatalog catalog;
	private ColumnDef colDef;

	public SystemFunctionHandlerTests() {
		ColumnName colName = mock(ColumnName.class);
		QueryColumnInfo colInfo = mock(QueryColumnInfo.class);
		catName = new CatalogName("catalogName");
		builder = mock(SparqlQueryBuilder.class);
		catalog = mock(RdfCatalog.class);
		Column column = mock(Column.class);
		colDef = mock(ColumnDef.class);
		when(builder.getCatalog()).thenReturn(catalog);
		when(catalog.getName()).thenReturn(catName);
		when(builder.getColumn((ColumnName) any())).thenReturn(colInfo);
		when(colInfo.getColumn()).thenReturn(column);
		when(column.getColumnDef()).thenReturn(colDef);
	}

	@Before
	public void setup() {
		func = new Function();
		stack = new Stack<Expr>();
		handler = new SystemFunctionHandler(builder, stack);
	}

	@Test
	public void testCatalogFunction() throws SQLException {
		Expr expr = null;
		func.setName("catalog");
		assertNotNull(handler.handle(func));
		expr = stack.pop();
		assertTrue(expr instanceof NodeValueString);
		assertEquals("catalogName", ((NodeValueString) expr).asString());

		func.setName("CATALOG");
		assertNotNull(handler.handle(func));
		expr = stack.pop();
		assertTrue(expr instanceof NodeValueString);
		assertEquals("catalogName", ((NodeValueString) expr).asString());

		func.setName("CaTaLoG");
		assertNotNull(handler.handle(func));
		expr = stack.pop();
		assertTrue(expr instanceof NodeValueString);
		assertEquals("catalogName", ((NodeValueString) expr).asString());
	}

	@Test
	public void testVersionFunction() throws SQLException {
		Expr expr = null;
		func.setName("version");
		assertNotNull(handler.handle(func));
		expr = stack.pop();
		assertTrue(expr instanceof NodeValueString);
		assertEquals(J4SDriver.getVersion(),
				((NodeValueString) expr).asString());

		func.setName("VERSION");
		assertNotNull(handler.handle(func));
		expr = stack.pop();
		assertTrue(expr instanceof NodeValueString);
		assertEquals(J4SDriver.getVersion(),
				((NodeValueString) expr).asString());

		func.setName("VerSion");
		assertNotNull(handler.handle(func));
		expr = stack.pop();
		assertTrue(expr instanceof NodeValueString);
		assertEquals(J4SDriver.getVersion(),
				((NodeValueString) expr).asString());
	}

}
