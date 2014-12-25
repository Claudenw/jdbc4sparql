package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class SchemaNameConstructorTests {

	private final String catalog = "catalog";
	private final String schema = "schema";

	@Test
	public void testNullSchema() {
		try {
			new SchemaName(catalog, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment schema may not be null", e.getMessage());
		}

	}

	@Test
	public void testNullCatalog() {
		try {
			new SchemaName(null, schema);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}

		try {
			new SchemaName((String) null, null);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}

	}
}
