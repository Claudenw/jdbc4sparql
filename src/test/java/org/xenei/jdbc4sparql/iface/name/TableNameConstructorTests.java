package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.*;

import org.junit.Test;

public class TableNameConstructorTests {

	private String catalog = "catalog";
	private String schema = "schema";
	private String table = "table";

	@Test
	public void testNullTable() {
		try {
			new TableName(catalog, schema, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment table may not be null", e.getMessage());
		}

	}

	@Test
	public void testNullSchema() {
		try {
			new TableName(catalog, null, table);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}

		try {
			new TableName(catalog, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}

	}

	@Test
	public void testNullCatalog() {
		try {
			new TableName(null, schema, table);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}

		try {
			new TableName(null, schema, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}

		try {
			new TableName(null, null, table);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}

		try {
			new TableName(null, null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}

	}
}
