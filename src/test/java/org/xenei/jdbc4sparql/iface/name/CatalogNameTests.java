package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.xenei.jdbc4sparql.impl.NameUtils;

public class CatalogNameTests {
	private CatalogName catalogName;

	public CatalogNameTests() {
	}

	@Test
	public void testCatalog() {
		catalogName = new CatalogName("catalog");
		assertEquals("catalog", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getColumn());
		assertNull(catalogName.getTable());
		assertEquals("catalog", catalogName.getDBName());
		assertEquals("catalog", catalogName.getSPARQLName());
		assertEquals("catalog", catalogName.getShortName());
	}

	@Test
	public void testCatalogFromCatalog() {
		catalogName = new CatalogName(new CatalogName("catalog"));
		assertEquals("catalog", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getColumn());
		assertNull(catalogName.getTable());
		assertEquals("catalog", catalogName.getDBName());
		assertEquals("catalog", catalogName.getSPARQLName());
		assertEquals("catalog", catalogName.getShortName());
	}

	@Test
	public void testCatalogFromOtherItemName() {
		final ItemName itemName = new ColumnName("catalog", "schema", "table",
				"column");
		catalogName = new CatalogName(itemName);
		assertEquals("catalog", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getColumn());
		assertNull(catalogName.getTable());
		assertEquals("catalog", catalogName.getDBName());
		assertEquals("catalog", catalogName.getSPARQLName());
		assertEquals("catalog", catalogName.getShortName());

	}

	@Test
	public void testCatalogWithDBDot() {
		try {
			catalogName = new CatalogName("cata" + NameUtils.DB_DOT + "log");
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			assertEquals("Catalog name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testCatalogWithSPARQLDot() {
		try {
			catalogName = new CatalogName("cata" + NameUtils.SPARQL_DOT + "log");
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			assertEquals("Catalog name may not contain '"
					+ NameUtils.SPARQL_DOT + "'", expected.getMessage());
		}
	}

	@Test
	public void testEmptyCatalog() {
		catalogName = new CatalogName("");
		assertEquals("", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getColumn());
		assertNull(catalogName.getTable());
		assertEquals("", catalogName.getDBName());
		assertEquals("", catalogName.getSPARQLName());
		assertEquals("", catalogName.getShortName());
	}

	@Test
	public void testEmptyCatalogFromCatalog() {
		catalogName = new CatalogName(new CatalogName(""));
		assertEquals("", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getColumn());
		assertNull(catalogName.getTable());
		assertEquals("", catalogName.getDBName());
		assertEquals("", catalogName.getSPARQLName());
		assertEquals("", catalogName.getShortName());
	}

}
