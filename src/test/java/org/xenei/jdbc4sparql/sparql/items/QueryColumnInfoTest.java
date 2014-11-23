package org.xenei.jdbc4sparql.sparql.items;

import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

import static org.junit.Assert.*;

public class QueryColumnInfoTest {

	private QueryColumnInfo columnInfo;
	private Column column;
	private ColumnName columnName;

	@Before
	public void setup() {
		column = mock(Column.class);
		columnName = new ColumnName("catalog", "schema", "table", "column");
		when(column.getName()).thenReturn(columnName);
		columnInfo = new QueryColumnInfo(column, false);
	}

	@Test
	public void testGetExpr() {
		assertNull(columnInfo.getExpr());
		columnInfo.setExpr(new NodeValueString("foo"));
		assertEquals(new NodeValueString("foo"), columnInfo.getExpr());
	}

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
		columnInfo.setSegments(new NameSegments(false, false, true, false));
		assertEquals("C:false S:false T:true C:true", columnInfo.getSegments()
				.toString());
		assertEquals("table.column", columnInfo.getName().getDBName());
		columnInfo.setSegments(new NameSegments(true, true, true, false));
		assertEquals("C:true S:true T:true C:true", columnInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", columnInfo.getName().getDBName());

	}

	@Test
	public void testGetName() {
		ItemName name = columnInfo.getName();
		assertTrue(name == columnName);
	}

	@Test
	public void testGetVar() {
		String dbName = "schema" + NameUtils.SPARQL_DOT + "table"
				+ NameUtils.SPARQL_DOT + "column";
		Var v = columnInfo.getVar();
		assertEquals(dbName, v.getName());
	}

	@Test
	public void testGetAlias() {
		String varName = columnInfo.getName().getGUID();
		assertNotNull(columnInfo.getGUIDVar());
		assertEquals(varName, columnInfo.getGUIDVar().getVarName());
	}

	@Test
	public void testIsOptional() {
		assertFalse(columnInfo.isOptional());
		columnInfo.setOptional(true);
		assertTrue(columnInfo.isOptional());
	}
}
