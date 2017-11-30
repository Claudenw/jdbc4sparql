package org.xenei.jdbc4sparql.sparql.items;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.NameSegments;
import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.impl.NameUtils;

import org.apache.jena.sparql.core.Var;

public class QueryItemInfoTest {

	private QueryItemInfo<NamedObject<ItemName>, ItemName> itemInfo;
	private ItemName itemName;
	private NamedObject<ItemName> namedObject;

	@Before
	public void setup() {

		itemName = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		itemName.setUsedSegments(NameSegments.ALL);

		namedObject = new NamedObject<ItemName>() {

			@Override
			public ItemName getName() {
				return itemName;
			}
		};

		itemInfo = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
				namedObject, itemName, false) {
		};
	}

	@Test
	public void testSegments() {
		assertEquals("TTTT", itemInfo.getSegments()
				.toString());
		assertEquals("catalog.schema.table.column", itemInfo.getName()
				.getDBName());
		itemInfo.setSegments(NameSegments.CATALOG);
		assertEquals("TFFF", itemInfo.getSegments()
				.toString());
		assertEquals("catalog", itemInfo.getName().getDBName());
		itemInfo.setSegments(NameSegments.SCHEMA);
		assertEquals("FTFF", itemInfo.getSegments()
				.toString());
		assertEquals("schema", itemInfo.getName().getDBName());
		itemInfo.setSegments(NameSegments.TABLE);
		assertEquals("FTTF", itemInfo.getSegments()
				.toString());
		assertEquals("schema.table", itemInfo.getName().getDBName());
		itemInfo.setSegments(NameSegments.COLUMN);
		assertEquals("FTTT", itemInfo.getSegments()
				.toString());
		assertEquals("schema.table.column", itemInfo.getName().getDBName());
	}

	@Test
	public void testGetName() {
		final ItemName name = itemInfo.getName();
		assertTrue(name == itemName);
	}

	@Test
	public void testGetVar() {
		assertEquals(GUIDObject.asVar(itemName), itemInfo.getGUIDVar());
	}

	@Test
	public void testGetAlias() {
		final Var varName = GUIDObject.asVar(itemInfo.getName());
		assertNotNull(itemInfo.getGUIDVar());
		assertEquals(varName, itemInfo.getGUIDVar());
	}

	@Test
	public void testIsOptional() {
		assertFalse(itemInfo.isOptional());
		itemInfo.setOptional(true);
		assertTrue(itemInfo.isOptional());
	}

	@Test
	public void testEquality() {
		QueryItemInfo<NamedObject<ItemName>, ItemName> itemInfo2;
		ItemName itemName2;
		final boolean[] tf = {
				true, false
		};
		for (final boolean catalogFlg : tf) {
			for (final boolean schemaFlg : tf) {
				for (final boolean tableFlg : tf) {
					for (final boolean columnFlg : tf) {
						itemName2 = itemName.clone(
								NameSegments.getInstance(catalogFlg, schemaFlg,
										tableFlg, columnFlg));
						itemInfo2 = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
								namedObject, itemName2, false) {
						};
						if (catalogFlg && schemaFlg && tableFlg && columnFlg) {
							assertEquality(itemInfo, itemInfo2);
						}
						else {
							assertNotEquals(itemInfo, itemInfo2);
							assertNotEquals(itemInfo2, itemInfo);
							assertEquals(itemInfo.hashCode(),
									itemInfo2.hashCode());
						}

						itemInfo2 = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
								namedObject, itemName2, true) {
						};
						if (catalogFlg && schemaFlg && tableFlg && columnFlg) {
							assertEquality(itemInfo, itemInfo2);
						}
						else {
							assertNotEquals(itemInfo, itemInfo2);
							assertNotEquals(itemInfo2, itemInfo);
							assertEquals(itemInfo.hashCode(),
									itemInfo2.hashCode());
						}
					}
				}
			}
		}
	}

	private void assertEquality(final QueryItemInfo<?, ?> item1,
			final QueryItemInfo<?, ?> item2) {
		assertEquals(item1, item2);
		assertEquals(item2, item1);
		assertEquals(item1.hashCode(), item2.hashCode());
	}
}
