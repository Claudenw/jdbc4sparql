package org.xenei.jdbc4sparql.sparql.items;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLDataException;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.sparql.CheckTypeF;
import org.xenei.jdbc4sparql.sparql.ForceTypeF;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.syntax.ElementBind;

public class QueryColumnInfoTest {

	private QueryColumnInfo columnInfo;
	private Column column;
	private ColumnName columnName;
	private ColumnDef colDef;

	@Before
	public void setup() {
		columnName = new ColumnName("catalog", "schema", "table", "column");

		colDef = mock(ColumnDef.class);
		when(colDef.getType()).thenReturn(java.sql.Types.VARCHAR);

		column = mock(Column.class);
		when(column.getName()).thenReturn(columnName);
		when(column.getColumnDef()).thenReturn(colDef);

		columnInfo = new QueryColumnInfo(column, false);
	}

	// @Test
	// public void testGetExpr() {
	// assertNull(columnInfo.getExpr());
	// columnInfo.setExpr(new NodeValueString("foo"));
	// assertEquals(new NodeValueString("foo"), columnInfo.getExpr());
	// }

	@Test
	public void testSegments() {
		assertEquals("C:false S:true T:true C:true", columnInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.CATALOG);
		assertEquals("C:true S:false T:false C:true", columnInfo.getSegments()
				.toString());
		assertEquals("column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.SCHEMA);
		assertEquals("C:false S:true T:true C:true", columnInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.TABLE);
		assertEquals("C:false S:true T:true C:true", columnInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.FFTF);
		assertEquals("C:false S:false T:true C:true", columnInfo.getSegments()
				.toString());
		assertEquals("table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(NameSegments.TTTF);
		assertEquals("C:true S:true T:true C:true", columnInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", columnInfo.getName().getDBName());

	}

	@Test
	public void testGetName() {
		final ItemName name = columnInfo.getName();
		assertTrue(name == columnName);
	}

	@Test
	public void testGetVar() {
		final String dbName = "schema" + NameUtils.SPARQL_DOT + "table"
				+ NameUtils.SPARQL_DOT + "column";
		final Var v = columnInfo.getVar();
		assertEquals(dbName, v.getName());
	}

	@Test
	public void testGetGUID() {
		final String varName = columnInfo.getName().getGUID();
		assertNotNull(columnInfo.getGUIDVar());
		assertEquals(varName, columnInfo.getGUIDVar().getVarName());
	}

	@Test
	public void testIsOptional() {
		assertFalse(columnInfo.isOptional());
		columnInfo.setOptional(true);
		assertTrue(columnInfo.isOptional());
	}

	@Test
	public void testAddAlias() throws SQLDataException {
		final ColumnName alias = new ColumnName("", "", "", "alias");
		final QueryColumnInfo aliasInfo = columnInfo.createAlias(alias);
		assertEquals(alias, aliasInfo.getName());
		final CheckTypeF cf = aliasInfo.getTypeFilter();
		assertEquals(columnInfo.getTypeFilter(), cf);
		assertNotEquals(columnInfo.getGUID(), alias.getGUID());
		assertEquals(columnInfo.getGUIDVar(), cf.getArg().asVar());

		final ForceTypeF ff = columnInfo.getDataFilter();
		assertEquals(columnInfo.getDataFilter(), ff);
		final ElementBind bind = ff.getBinding(aliasInfo);
		assertEquals(aliasInfo.getVar(), bind.getVar());

		final Var v = ((ExprVar) ((ForceTypeF) bind.getExpr()).getArg())
				.asVar();
		assertEquals(columnInfo.getGUIDVar(), v);

	}

	@Test
	public void testGetColumn() {
		assertEquals(column, columnInfo.getColumn());
	}

	@Test
	public void testGetTypeFilter() throws SQLDataException {
		final CheckTypeF f = columnInfo.getTypeFilter();
		assertEquals(columnInfo, f.getColumnInfo());
		assertEquals(columnInfo.getGUIDVar(), f.getArg().asVar());
	}

	@Test
	public void testGetDataFilter() throws SQLDataException {
		final ForceTypeF f = columnInfo.getDataFilter();
		final ElementBind bind = f.getBinding(columnInfo);
		assertEquals(columnInfo.getVar(), bind.getVar());
		final Var v = ((ExprVar) ((ForceTypeF) bind.getExpr()).getArg())
				.asVar();
		assertEquals(columnInfo.getGUIDVar(), v);
	}

}
