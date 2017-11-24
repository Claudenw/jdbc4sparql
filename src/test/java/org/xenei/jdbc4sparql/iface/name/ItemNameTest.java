package org.xenei.jdbc4sparql.iface.name;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.impl.NameUtils;

public class ItemNameTest {
	private ItemName itemName;

	@Before
	public void setup() {
		itemName = new ItemName("catalog", "schema", "table", "column", NameSegments.ALL) {

			@Override
			public ItemName clone(NameSegments segs) {
				// TODO Auto-generated method stub
				return null;
			}};
	}

	@Test
	public void testSegments() {
		assertEquals("C:true S:true T:true C:true", itemName.getUsedSegments()
				.toString());
		assertEquals("catalog.schema.table.column", itemName.getDBName());
		itemName.setUsedSegments(NameSegments.CATALOG);
		assertEquals("C:true S:false T:false C:false", itemName
				.getUsedSegments().toString());
//		assertEquals("catalog.null.null.null", itemName.getDBName());
		assertEquals("catalog", itemName.getDBName());
		itemName.setUsedSegments(NameSegments.SCHEMA);
		assertEquals("C:false S:true T:false C:false", itemName
				.getUsedSegments().toString());
//		assertEquals("null.schema.null.null", itemName.getDBName());
		assertEquals("schema", itemName.getDBName());
		itemName.setUsedSegments(NameSegments.TABLE);
		assertEquals("C:false S:true T:true C:false", itemName
				.getUsedSegments().toString());
//		assertEquals("null.schema.table.null", itemName.getDBName());
		assertEquals("schema.table", itemName.getDBName());
		itemName.setUsedSegments(NameSegments.COLUMN);
		assertEquals("C:false S:true T:true C:true", itemName.getUsedSegments()
				.toString());
//		assertEquals("null.schema.table.column", itemName.getDBName());
		assertEquals("schema.table.column", itemName.getDBName());
	}

	@Test
	public void testCreateName() {
		assertEquals("catalog,schema,table,column", itemName.createName(","));
		assertEquals("catalog:schema:table:column", itemName.createName(":"));
		assertEquals("catalogschematablecolumn", itemName.createName(""));
	}

	@Test
	public void testGetBaseName() {
		final FQName base = itemName.getFQName();
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
		final String testName = "catalog" + NameUtils.DB_DOT + "schema"
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
		final String dbName = "catalog" + NameUtils.SPARQL_DOT + "schema"
				+ NameUtils.SPARQL_DOT + "table" + NameUtils.SPARQL_DOT
				+ "column";
		assertEquals(dbName, itemName.getSPARQLName());
	}

	@Test
	public void testGetGUID() {
		assertEquals(itemName.getFQName().getGUID(), itemName.getGUID());
		itemName.setUsedSegments(NameSegments.FTFF);
		assertEquals(itemName.getFQName().getGUID(), itemName.getGUID());
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
		ColumnName tn = new ColumnName("catalog", "schema", "table", "column",NameSegments.ALL);
		assertTrue(itemName.matches(tn));
		tn.setUsedSegments(NameSegments.TTTF);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTFT);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TFTT);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.FTTT);

		tn = new ColumnName("catalog", "schema", "table", "column1",NameSegments.ALL);
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTTF);
		assertTrue(tn.matches(itemName));

		tn = new ColumnName("catalog", "schema", "table1", "column",NameSegments.ALL);
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTFT);
		assertTrue(tn.matches(itemName));

		tn = new ColumnName("catalog", "schema1", "table", "column",NameSegments.ALL);
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TFTT);
		assertTrue(tn.matches(itemName));

		tn = new ColumnName("catalog1", "schema", "table", "column",NameSegments.ALL);
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.FTTT);
		assertTrue(tn.matches(itemName));
	}

	@Test
	public void testShorterMatches() {
		itemName.setUsedSegments(NameSegments.TTTF);
		assertFalse(itemName.matches(null));
		ColumnName tn = new ColumnName("catalog", "schema", "table", "column", NameSegments.ALL);
		assertTrue(itemName.matches(tn));
		tn.setUsedSegments(NameSegments.TTTF);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTFT);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TFTT);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.FTTT);

		tn = new ColumnName("catalog", "schema", "table", "column1", NameSegments.ALL);
		assertTrue(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTTF);
		assertTrue(tn.matches(itemName));

		tn = new ColumnName("catalog", "schema", "table1", "column", NameSegments.ALL);
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TTFT);
		assertTrue(tn.matches(itemName));

		tn = new ColumnName("catalog", "schema1", "table", "column", NameSegments.ALL);
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.TFTT);
		assertTrue(tn.matches(itemName));

		tn = new ColumnName("catalog1", "schema", "table", "column", NameSegments.ALL);
		assertFalse(tn.matches(itemName));
		tn.setUsedSegments(NameSegments.FTTT);
		assertTrue(tn.matches(itemName));
	}

	@Test
	public void testEquality() {
		ItemName itemName2;
		final boolean[] tf = {
				true, false
		};
		for (final boolean catalogFlg : tf) {
			for (final boolean schemaFlg : tf) {
				for (final boolean tableFlg : tf) {
					for (final boolean columnFlg : tf) {
						itemName2 = new ColumnName(itemName,
								NameSegments.getInstance(catalogFlg, schemaFlg,
										tableFlg, columnFlg));
						if (catalogFlg && schemaFlg && tableFlg && columnFlg) {
							assertEquals(itemName, itemName2);
							assertEquals(itemName2, itemName);
						}
						else {
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
