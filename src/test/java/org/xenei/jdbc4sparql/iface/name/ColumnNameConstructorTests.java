package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class ColumnNameConstructorTests {

	private final String catalog = "catalog";
	private final String schema = "schema";
	private final String table = "table";
	private final String column = "column";

	@Test
	public void testNullColumn() {
		try {
			new ColumnName(catalog, schema, table, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment column may not be null", e.getMessage());
		}
	}

	@Test
	public void testNullTable() {
		try {
			new ColumnName(catalog, schema, null, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment table may not be null", e.getMessage());
		}
		try {
			new ColumnName(catalog, schema, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment table may not be null", e.getMessage());
		}
	}

	@Test
	public void testNullSchema() {
		try {
			new ColumnName(catalog, null, table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
		try {
			new ColumnName(catalog, null, table, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
		try {
			new ColumnName(catalog, null, null, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
		try {
			new ColumnName(catalog, null, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}
	}

	@Test
	public void testNullCatalog() {
		try {
			new ColumnName(null, schema, table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, schema, table, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, schema, null, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, schema, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, null, table, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, null, table, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, null, null, column);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
		try {
			new ColumnName(null, null, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}
	}

	@Test
	public void testGetInstance() {
		final ColumnName cName = new ColumnName("catalog", "schema", "table",
				"column");
		// standard segment configurations.
		final NameSegments segs[] = {
				NameSegments.TTTT, NameSegments.FTTT, NameSegments.FFTT,
				 NameSegments.FFFT
		};
		for (final NameSegments seg : segs) {
			final ColumnName sn = cName.clone(seg);
			ColumnName result = ColumnName.getNameInstance("catalog2",
					"schema2", "table2", sn.getSPARQLName());
			// make it so we can see all the columns
			ColumnName answer = result.clone(NameSegments.ALL);
			assertEquals(sn.getSPARQLName(), "catalog2" ,  answer.getCatalog());
			assertEquals(sn.getSPARQLName(), seg.isSchema() ? "schema" : "schema2", answer.getSchema());
			assertEquals(sn.getSPARQLName(),
					seg.isTable() ? "table" : "table2",
					answer.getTable());
			assertEquals(sn.getSPARQLName(), column, answer.getColumn());

			result = ColumnName.getNameInstance("catalog2", "schema2",
					"table2", sn.getDBName());
			answer = result.clone(NameSegments.ALL);
			assertEquals(sn.getSPARQLName(), "catalog2", answer.getCatalog());
			assertEquals(sn.getSPARQLName(), seg.isSchema() ? "schema"
					: "schema2", answer.getSchema());
			assertEquals(sn.getSPARQLName(),
					seg.isTable() ? "table" : "table2",
					answer.getTable());
			assertEquals(sn.getSPARQLName(), column, answer.getColumn());
		}

	}
}
