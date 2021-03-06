package org.xenei.jdbc4sparql.sparql.items;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.SearchName;
import org.xenei.jdbc4sparql.impl.NameUtils;

import com.hp.hpl.jena.sparql.core.Var;

public class QueryItemInfoTest {

	private QueryItemInfo<NamedObject<ItemName>, ItemName> itemInfo;
	private ItemName itemName;
	private NamedObject<ItemName> namedObject;

	@Before
	public void setup() {

		itemName = new SearchName("catalog", "schema", "table", "column");
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
		assertEquals("C:true S:true T:true C:true", itemInfo.getSegments()
				.toString());
		assertEquals("catalog.schema.table.column", itemInfo.getName()
				.getDBName());
		itemInfo.setSegments(NameSegments.CATALOG);
		assertEquals("C:true S:false T:false C:false", itemInfo.getSegments()
				.toString());
		assertEquals("catalog.null.null.null", itemInfo.getName().getDBName());
		itemInfo.setSegments(NameSegments.SCHEMA);
		assertEquals("C:false S:true T:false C:false", itemInfo.getSegments()
				.toString());
		assertEquals("null.schema.null.null", itemInfo.getName().getDBName());
		itemInfo.setSegments(NameSegments.TABLE);
		assertEquals("C:false S:true T:true C:false", itemInfo.getSegments()
				.toString());
		assertEquals("null.schema.table.null", itemInfo.getName().getDBName());
		itemInfo.setSegments(NameSegments.COLUMN);
		assertEquals("C:false S:true T:true C:true", itemInfo.getSegments()
				.toString());
		assertEquals("null.schema.table.column", itemInfo.getName().getDBName());
	}

	@Test
	public void testGetName() {
		final ItemName name = itemInfo.getName();
		assertTrue(name == itemName);
	}

	@Test
	public void testGetVar() {
		final String dbName = "catalog" + NameUtils.SPARQL_DOT + "schema"
				+ NameUtils.SPARQL_DOT + "table" + NameUtils.SPARQL_DOT
				+ "column";
		final Var v = itemInfo.getVar();
		assertEquals(dbName, v.getName());
	}

	@Test
	public void testGetAlias() {
		final String varName = itemInfo.getName().getGUID();
		assertNotNull(itemInfo.getGUIDVar());
		assertEquals(varName, itemInfo.getGUIDVar().getVarName());
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
						itemName2 = new SearchName(itemName,
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
