package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.*;

import org.junit.Test;

public class ColumnNameConstructorTests {

	private String catalog = "catalog";
	private String schema = "schema";
	private String table = "table";
	private String column = "column";

	@Test
	public void testNullColumn() {
		try {
			new ColumnName(catalog, schema, table, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment column may not be null", e.getMessage());
		}
	}

	@Test
	public void testNullTable() {
		try {
			new ColumnName(catalog, schema, null, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment table may not be null", e.getMessage());
		}
		try {
			new ColumnName(catalog, schema, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment table may not be null", e.getMessage());
		}
	}

	@Test
	public void testNullSchema() {
		try {
			new ColumnName(catalog, null, table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
		try {
			new ColumnName(catalog, null, table, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
		try {
			new ColumnName(catalog, null, null, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
		try {
			new ColumnName(catalog, null, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
	}

	@Test
	public void testNullCatalog() {
		try {
			new ColumnName(null, schema, table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, schema, table, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, schema, null, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, schema, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, null, table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, null, table, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, null, null, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, null, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
	}

	@Test
	public void testGetInstance() {
		ColumnName cName = new ColumnName("catalog", "schema", "table",
				"column");
		NameSegments segs[] = { new NameSegments(true, true, true, true),
				new NameSegments(false, true, true, true),
				new NameSegments(true, false, true, true),
				new NameSegments(false, false, true, true),
				new NameSegments(true, false, false, true),
				new NameSegments(false, false, false, true), };

		for (NameSegments seg : segs) {
			ColumnName sn = cName.clone(seg);
			ColumnName result = ColumnName.getNameInstance("catalog2",
					"schema2", "table2", sn.getSPARQLName());
			ColumnName answer = result.clone(NameSegments.ALL);
			assertEquals(sn.getSPARQLName(), "catalog2", answer.getCatalog());
			assertEquals(sn.getSPARQLName(), seg.isSchema() ? "schema"
					: "schema2", answer.getSchema());
			assertEquals(sn.getSPARQLName(),
					seg.isSchema() || seg.isTable() ? "table" : "table2",
					answer.getTable());
			assertEquals(sn.getSPARQLName(), column, answer.getCol());

			result = ColumnName.getNameInstance("catalog2", "schema2",
					"table2", sn.getDBName());
			answer = result.clone(NameSegments.ALL);
			assertEquals(sn.getSPARQLName(), "catalog2", answer.getCatalog());
			assertEquals(sn.getSPARQLName(), seg.isSchema() ? "schema"
					: "schema2", answer.getSchema());
			assertEquals(sn.getSPARQLName(),
					seg.isSchema() || seg.isTable() ? "table" : "table2",
					answer.getTable());
			assertEquals(sn.getSPARQLName(), column, answer.getCol());
		}

	}
}
