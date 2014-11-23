package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.impl.NameUtils;

public class ItemNameTest {
	public ItemName itemName;

	public static class TestName extends ItemName {
		public TestName() {
			this("catalog", "schema", "table", "column");
		}

		public TestName(String catalog, String schema, String table,
				String column) {
			this(catalog, schema, table, column, new NameSegments(true, true,
					true, true));
		}

		public TestName(String catalog, String schema, String table,
				String column, NameSegments segs) {
			super(catalog, schema, table, column);
			setUsedSegments(segs);
		}

		public TestName(ItemName itemName, NameSegments segs) {
			super(itemName, segs);
		}

		@Override
		protected String createName(String separator) {
			return String.format("%s%s%s%s%s%s%s", getCatalog(), separator,
					getSchema(), separator, getTable(), separator, getCol());
		}

		@Override
		public String getShortName() {
			return "shortName";
		}

		public RenamedBaseName rename(String catalog, String schema,
				String table, String column) {
			return new RenamedBaseName(getBaseName(), catalog, schema, table,
					column);
		}

		@Override
		public ItemName clone(NameSegments segs) {
			return new TestName(this, segs);
		}

	}

	@Before
	public void setup() {
		itemName = new TestName();
	}

	@Test
	public void testSegments() {
		assertEquals("C:true S:true T:true C:true", itemName.getUsedSegments()
				.toString());
		assertEquals("catalog.schema.table.column", itemName.getDBName());
		itemName.setUsedSegments(NameSegments.CATALOG);
		assertEquals("C:true S:false T:false C:false", itemName
				.getUsedSegments().toString());
		assertEquals("catalog.null.null.null", itemName.getDBName());
		itemName.setUsedSegments(NameSegments.SCHEMA);
		assertEquals("C:false S:true T:false C:false", itemName
				.getUsedSegments().toString());
		assertEquals("null.schema.null.null", itemName.getDBName());
		itemName.setUsedSegments(NameSegments.TABLE);
		assertEquals("C:false S:true T:true C:false", itemName
				.getUsedSegments().toString());
		assertEquals("null.schema.table.null", itemName.getDBName());
		itemName.setUsedSegments(NameSegments.COLUMN);
		assertEquals("C:false S:true T:true C:true", itemName.getUsedSegments()
				.toString());
		assertEquals("null.schema.table.column", itemName.getDBName());
	}

	@Test
	public void testCreateName() {
		assertEquals("catalog,schema,table,column", itemName.createName(","));
		assertEquals("catalog:schema:table:column", itemName.createName(":"));
		assertEquals("catalogschematablecolumn", itemName.createName(""));
	}

	@Test
	public void testGetBaseName() {
		BaseName base = itemName.getBaseName();
		assertEquals("catalog", base.getCatalog());
		assertEquals("schema", base.getSchema());
		assertEquals("table", base.getTable());
		assertEquals("column", base.getCol());
	}

	@Test
	public void testGetCol() {
		itemName.setUsedSegments(new NameSegments(false, false, false, false));
		assertNull(itemName.getCol());
		itemName.setUsedSegments(new NameSegments(false, false, false, true));
		assertEquals("column", itemName.getCol());
	}

	@Test
	public void testGetDBName() {
		String testName = "catalog" + NameUtils.DB_DOT + "schema"
				+ NameUtils.DB_DOT + "table" + NameUtils.DB_DOT + "column";
		assertEquals(testName, itemName.getDBName());
	}

	@Test
	public void getCatalog() {
		itemName.setUsedSegments(new NameSegments(false, false, false, false));
		assertNull(itemName.getCatalog());
		itemName.setUsedSegments(new NameSegments(true, false, false, false));
		assertEquals("catalog", itemName.getCatalog());
	}

	@Test
	public void testGetSchema() {
		itemName.setUsedSegments(new NameSegments(false, false, false, false));
		assertNull(itemName.getSchema());
		itemName.setUsedSegments(new NameSegments(false, true, false, false));
		assertEquals("schema", itemName.getSchema());
	}

	@Test
	public void testGetShortName() {
		assertEquals("shortName", itemName.getShortName());
	}

	@Test
	public void testGetSPARQLName() {
		String dbName = "catalog" + NameUtils.SPARQL_DOT + "schema"
				+ NameUtils.SPARQL_DOT + "table" + NameUtils.SPARQL_DOT
				+ "column";
		assertEquals(dbName, itemName.getSPARQLName());
	}

	@Test
	public void testGetGUID() {
		assertEquals(itemName.getBaseName().getGUID(), itemName.getGUID());
		itemName.setUsedSegments(new NameSegments(false, true, false, false));
		assertEquals(itemName.getBaseName().getGUID(), itemName.getGUID());
	}

	@Test
	public void testGetTable() {
		itemName.setUsedSegments(new NameSegments(false, false, false, false));
		assertNull(itemName.getTable());
		itemName.setUsedSegments(new NameSegments(false, false, true, false));
		assertEquals("table", itemName.getTable());
	}

	@Test
	public void testHasWild() {
		assertFalse(itemName.hasWild());
		itemName.setUsedSegments(new NameSegments(true, true, true, false));
		assertTrue(itemName.hasWild());
		itemName.setUsedSegments(new NameSegments(true, true, false, true));
		assertTrue(itemName.hasWild());
		itemName.setUsedSegments(new NameSegments(true, false, true, true));
		assertTrue(itemName.hasWild());
		itemName.setUsedSegments(new NameSegments(false, true, true, true));
		assertTrue(itemName.hasWild());
	}

	@Test
	public void testIsWild() {
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(new NameSegments(true, true, true, false));
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(new NameSegments(true, true, false, true));
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(new NameSegments(true, false, true, true));
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(new NameSegments(false, true, true, true));
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(new NameSegments(false, false, false, false));
		assertTrue(itemName.isWild());
	}

	// /**
	// * Find the object matching the key in the map.
	// * Uses matches() method to determine match.
	// *
	// * @param map
	// * The map to find the object in.
	// * @return The Object (T) or null if not found
	// * @throws IllegalArgumentException
	// * if more than one object matches.
	// */
	// public <T> Set<T> listMatches( final Map<? extends ItemName, T> map )
	// {
	// final Set<T> retval = new HashSet<T>();
	// for (final ItemName n : map.keySet())
	// {
	// if (matches(n))
	// {
	// retval.add(map.get(n));
	// }
	// }
	// return retval;
	// }
	//
	@Test
	public void testMatches() {
		assertFalse(itemName.matches(null));
		TestName tn = new TestName();
		assertTrue(itemName.matches(tn));
		tn.setUsedSegments(new NameSegments(true, true, true, false));
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(new NameSegments(true, true, false, true));
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(new NameSegments(true, false, true, true));
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(new NameSegments(false, true, true, true));

		tn = new TestName("catalog", "schema", "table", "column1");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(new NameSegments(true, true, true, false));
		assertTrue(tn.matches(itemName));

		tn = new TestName("catalog", "schema", "table1", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(new NameSegments(true, true, false, true));
		assertTrue(tn.matches(itemName));

		tn = new TestName("catalog", "schema1", "table", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(new NameSegments(true, false, true, true));
		assertTrue(tn.matches(itemName));

		tn = new TestName("catalog1", "schema", "table", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(new NameSegments(false, true, true, true));
		assertTrue(tn.matches(itemName));
	}

	//
	// @Override
	// public String toString()
	// {
	// if (isWild())
	// {
	// return "Wildcard Name";
	// }
	// return StringUtils.defaultIfBlank(getDBName(), "Blank Name");
	// }

	@Test
	public void testEquality() {
		ItemName itemName2;
		boolean[] tf = { true, false };
		for (boolean catalogFlg : tf) {
			for (boolean schemaFlg : tf) {
				for (boolean tableFlg : tf) {
					for (boolean columnFlg : tf) {
						itemName2 = new ItemNameTest.TestName(itemName,
								new NameSegments(catalogFlg, schemaFlg,
										tableFlg, columnFlg));
						if (catalogFlg && schemaFlg && tableFlg && columnFlg) {
							assertEquals(itemName, itemName2);
							assertEquals(itemName2, itemName);
						} else {
							assertNotEquals(itemName, itemName2);
							assertNotEquals(itemName2, itemName);
						}
						assertEquals(itemName.hashCode(), itemName2.hashCode());
					}
				}
			}
		}
	}
}
