package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.impl.NameUtils;

public class ItemNameTest {
	private ItemName itemName;

	@Before
	public void setup() {
		itemName = new SearchName("catalog", "schema", "table", "column");
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
		FQName base = itemName.getBaseName();
		assertEquals("catalog", base.getCatalog());
		assertEquals("schema", base.getSchema());
		assertEquals("table", base.getTable());
		assertEquals("column", base.getColumn());
	}

	@Test
	public void testGetCol() {
		itemName.setUsedSegments(NameSegments.FFFF);
		assertNull(itemName.getColumn());
		itemName.setUsedSegments(NameSegments.FFFT);
		assertEquals("column", itemName.getColumn());
	}

	@Test
	public void testGetDBName() {
		String testName = "catalog" + NameUtils.DB_DOT + "schema"
				+ NameUtils.DB_DOT + "table" + NameUtils.DB_DOT + "column";
		assertEquals(testName, itemName.getDBName());
	}

	@Test
	public void getCatalog() {
		itemName.setUsedSegments(NameSegments.FFFF);
		assertNull(itemName.getCatalog());
		itemName.setUsedSegments(NameSegments.TFFF);
		assertEquals("catalog", itemName.getCatalog());
	}

	@Test
	public void testGetSchema() {
		itemName.setUsedSegments(NameSegments.FFFF);
		assertNull(itemName.getSchema());
		itemName.setUsedSegments(NameSegments.FTFF);
		assertEquals("schema", itemName.getSchema());
	}

	@Test
	public void testGetShortName() {
		assertEquals("column", itemName.getShortName());
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
		itemName.setUsedSegments(NameSegments.FTFF);
		assertEquals(itemName.getBaseName().getGUID(), itemName.getGUID());
	}

	@Test
	public void testGetTable() {
		itemName.setUsedSegments(NameSegments.FFFF);
		assertNull(itemName.getTable());
		itemName.setUsedSegments(NameSegments.FFTF);
		assertEquals("table", itemName.getTable());
	}

	@Test
	public void testHasWild() {
		assertFalse(itemName.hasWild());
		itemName.setUsedSegments(NameSegments.TTTF);
		assertTrue(itemName.hasWild());
		itemName.setUsedSegments(NameSegments.TTFT);
		assertTrue(itemName.hasWild());
		itemName.setUsedSegments(NameSegments.TFTT);
		assertTrue(itemName.hasWild());
		itemName.setUsedSegments(NameSegments.FTTT);
		assertTrue(itemName.hasWild());
	}

	@Test
	public void testIsWild() {
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(NameSegments.TTTF);
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(NameSegments.TTFT);
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(NameSegments.TFTT);
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(NameSegments.FTTT);
		assertFalse(itemName.isWild());
		itemName.setUsedSegments(NameSegments.FFFF);
		assertTrue(itemName.isWild());
	}

	@Test
	public void testMatches() {
		assertFalse(itemName.matches(null));
		SearchName tn = new SearchName("catalog", "schema", "table", "column");
		assertTrue(itemName.matches(tn));
		tn.setUsedSegments(NameSegments.TTTF);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTFT);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TFTT);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.FTTT);

		tn = new SearchName("catalog", "schema", "table", "column1");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTTF);
		assertTrue(tn.matches(itemName));

		tn = new SearchName("catalog", "schema", "table1", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTFT);
		assertTrue(tn.matches(itemName));

		tn = new SearchName("catalog", "schema1", "table", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TFTT);
		assertTrue(tn.matches(itemName));

		tn = new SearchName("catalog1", "schema", "table", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.FTTT);
		assertTrue(tn.matches(itemName));
	}
	
	@Test
	public void testShorterMatches() {
		itemName.setUsedSegments(NameSegments.TTTF);
		assertFalse(itemName.matches(null));
		SearchName tn = new SearchName("catalog", "schema", "table", "column");
		assertTrue(itemName.matches(tn));
		tn.setUsedSegments(NameSegments.TTTF);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTFT);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TFTT);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.FTTT);

		tn = new SearchName("catalog", "schema", "table", "column1");
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTTF);
		assertTrue(tn.matches(itemName));

		tn = new SearchName("catalog", "schema", "table1", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTFT);
		assertTrue(tn.matches(itemName));

		tn = new SearchName("catalog", "schema1", "table", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TFTT);
		assertTrue(tn.matches(itemName));

		tn = new SearchName("catalog1", "schema", "table", "column");
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.FTTT);
		assertTrue(tn.matches(itemName));
	}

	@Test
	public void testEquality() {
		ItemName itemName2;
		boolean[] tf = { true, false };
		for (boolean catalogFlg : tf) {
			for (boolean schemaFlg : tf) {
				for (boolean tableFlg : tf) {
					for (boolean columnFlg : tf) {
						itemName2 = new SearchName(itemName,
								NameSegments.getInstance(catalogFlg, schemaFlg,
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
