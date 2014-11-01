package org.xenei.jdbc4sparql.iface;

import static org.junit.Assert.*;

import org.junit.Test;
import org.xenei.jdbc4sparql.impl.NameUtils;

public class CatalogNameTests
{
	private CatalogName catalogName;

	public CatalogNameTests()
	{
	}

	@Test
	public void testCatalog()
	{
		catalogName = new CatalogName("catalog");
		assertEquals("catalog", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getCol());
		assertNull(catalogName.getTable());
		assertEquals("catalog", catalogName.getDBName());
		assertEquals("catalog", catalogName.getSPARQLName());
		assertEquals("catalog", catalogName.getShortName());
	}

	@Test
	public void testCatalogFromCatalog()
	{
		catalogName = new CatalogName(new CatalogName("catalog"));
		assertEquals("catalog", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getCol());
		assertNull(catalogName.getTable());
		assertEquals("catalog", catalogName.getDBName());
		assertEquals("catalog", catalogName.getSPARQLName());
		assertEquals("catalog", catalogName.getShortName());
	}

	@Test
	public void testCatalogFromOtherItemName()
	{
		final ItemName itemName = new ItemName("schema", "table", "column") {

			@Override
			protected String createName( String separator )
			{
				return "NAME";
			}

			@Override
			public String getShortName()
			{
				return "SHORT";
			}
		};
		catalogName = new CatalogName(itemName);
		assertNull(catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getCol());
		assertNull(catalogName.getTable());
		assertEquals("", catalogName.getDBName());
		assertEquals("", catalogName.getSPARQLName());
		assertNull( catalogName.getShortName());
	}

	@Test
	public void testCatalogWithDBDot()
	{
		try
		{
			catalogName = new CatalogName("cata" + NameUtils.DB_DOT + "log");
			fail("Should have thrown IllegalArgumentException");
		}
		catch (final IllegalArgumentException expected)
		{
			assertEquals("Catalog name may not contain '.'",
					expected.getMessage());
		}

	}

	@Test
	public void testCatalogWithSPARQLDot()
	{
		try
		{
			catalogName = new CatalogName("cata" + NameUtils.SPARQL_DOT + "log");
			fail("Should have thrown IllegalArgumentException");
		}
		catch (final IllegalArgumentException expected)
		{
			assertEquals("Catalog name may not contain '"
					+ NameUtils.SPARQL_DOT + "'", expected.getMessage());
		}
	}

	@Test
	public void testEmptyCatalog()
	{
		catalogName = new CatalogName("");
		assertEquals("", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getCol());
		assertNull(catalogName.getTable());
		assertEquals("", catalogName.getDBName());
		assertEquals("", catalogName.getSPARQLName());
		assertEquals("", catalogName.getShortName());
	}

	@Test
	public void testEmptyCatalogFromCatalog()
	{
		catalogName = new CatalogName(new CatalogName(""));
		assertEquals("", catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getCol());
		assertNull(catalogName.getTable());
		assertEquals("", catalogName.getDBName());
		assertEquals("", catalogName.getSPARQLName());
		assertEquals("", catalogName.getShortName());
	}

	@Test
	public void testNullCatalog()
	{
		catalogName = new CatalogName((String) null);
		assertNull(catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getCol());
		assertNull(catalogName.getTable());
		assertEquals("", catalogName.getDBName());
		assertEquals("", catalogName.getSPARQLName());
		assertNull( catalogName.getShortName());
	}

	@Test
	public void testNullCatalogFromCatalog()
	{
		catalogName = new CatalogName(new CatalogName((String) null));
		assertNull(catalogName.getCatalog());
		assertNull(catalogName.getSchema());
		assertNull(catalogName.getCol());
		assertNull(catalogName.getTable());
		assertEquals("", catalogName.getDBName());
		assertEquals("", catalogName.getSPARQLName());
		assertNull( catalogName.getShortName());
	}
}
