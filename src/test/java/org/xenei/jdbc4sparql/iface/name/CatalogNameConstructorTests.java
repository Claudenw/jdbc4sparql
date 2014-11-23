package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.*;

import org.junit.Test;

public class CatalogNameConstructorTests {

	@Test
	public void testNullCatalog() {
		try {
			new CatalogName((String) null);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Segment catalog may not be null", e.getMessage());
		}

	}
}
