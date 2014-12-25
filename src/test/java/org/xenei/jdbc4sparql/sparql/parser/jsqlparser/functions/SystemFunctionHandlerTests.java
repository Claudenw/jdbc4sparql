package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

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
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.AbstractFunctionHandler.FuncInfo;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

public class SystemFunctionHandlerTests {
	private SystemFunctionHandler handler;
	private final SparqlQueryBuilder builder;
	private Function func;
	private final CatalogName catName;
	private final RdfCatalog catalog;
	private final ColumnDef colDef;
	private String alias;

	public SystemFunctionHandlerTests() {
		final QueryColumnInfo colInfo = mock(QueryColumnInfo.class);
		catName = new CatalogName("catalogName");
		builder = mock(SparqlQueryBuilder.class);
		catalog = mock(RdfCatalog.class);
		final Column column = mock(Column.class);
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
		handler = new SystemFunctionHandler(builder);
		alias = "Alias";
	}

	@Test
	public void testCatalogFunction() throws SQLException {
		func.setName("catalog");
		FuncInfo funcInfo = handler.handle(func, alias);
		assertEquals(alias, funcInfo.columnName.getShortName());
		Expr expr = funcInfo.expr;
		assertTrue(expr instanceof NodeValueString);
		assertEquals("catalogName", ((NodeValueString) expr).asString());

		func.setName("CATALOG");
		funcInfo = handler.handle(func, alias);
		assertEquals(alias, funcInfo.columnName.getShortName());
		expr = funcInfo.expr;
		assertTrue(expr instanceof NodeValueString);
		assertEquals("catalogName", ((NodeValueString) expr).asString());

		func.setName("CaTaLoG");
		funcInfo = handler.handle(func, alias);
		assertEquals(alias, funcInfo.columnName.getShortName());
		expr = funcInfo.expr;
		assertTrue(expr instanceof NodeValueString);
		assertEquals("catalogName", ((NodeValueString) expr).asString());
	}

	@Test
	public void testVersionFunction() throws SQLException {
		func.setName("version");
		FuncInfo funcInfo = handler.handle(func, alias);
		assertEquals(alias, funcInfo.columnName.getShortName());
		Expr expr = funcInfo.expr;
		assertTrue(expr instanceof NodeValueString);
		assertEquals(J4SDriver.getVersion(),
				((NodeValueString) expr).asString());

		func.setName("VERSION");
		funcInfo = handler.handle(func, alias);
		assertEquals(alias, funcInfo.columnName.getShortName());
		expr = funcInfo.expr;
		assertTrue(expr instanceof NodeValueString);
		assertEquals(J4SDriver.getVersion(),
				((NodeValueString) expr).asString());

		func.setName("VerSion");
		funcInfo = handler.handle(func, alias);
		assertEquals(alias, funcInfo.columnName.getShortName());
		expr = funcInfo.expr;
		assertTrue(expr instanceof NodeValueString);
		assertEquals(J4SDriver.getVersion(),
				((NodeValueString) expr).asString());
	}

}
