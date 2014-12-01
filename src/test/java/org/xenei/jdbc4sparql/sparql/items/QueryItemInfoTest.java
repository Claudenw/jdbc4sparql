package org.xenei.jdbc4sparql.sparql.items;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.ItemNameTest;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.SearchName;
import org.xenei.jdbc4sparql.impl.NameUtils;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

import static org.junit.Assert.*;

public class QueryItemInfoTest {

	private QueryItemInfo<ItemName> itemInfo;
	private ItemName itemName;

	@Before
	public void setup() {
		itemName = new SearchName("catalog", "schema", "table",
				"column");
		itemName.setUsedSegments(new NameSegments(true, true, true, true));
		itemInfo = new QueryItemInfo<ItemName>(itemName, false) {
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
		ItemName name = itemInfo.getName();
		assertTrue(name == itemName);
	}

	@Test
	public void testGetVar() {
		String dbName = "catalog" + NameUtils.SPARQL_DOT + "schema"
				+ NameUtils.SPARQL_DOT + "table" + NameUtils.SPARQL_DOT
				+ "column";
		Var v = itemInfo.getVar();
		assertEquals(dbName, v.getName());
	}

	@Test
	public void testGetAlias() {
		String varName = itemInfo.getName().getGUID();
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
		QueryItemInfo<ItemName> itemInfo2;
		ItemName itemName2;
		boolean[] tf = { true, false };
		for (boolean catalogFlg : tf) {
			for (boolean schemaFlg : tf) {
				for (boolean tableFlg : tf) {
					for (boolean columnFlg : tf) {
						itemName2 = new SearchName(itemName,
								new NameSegments(catalogFlg, schemaFlg,
										tableFlg, columnFlg));
						itemInfo2 = new QueryItemInfo<ItemName>(itemName2,
								false) {
						};
						if (catalogFlg && schemaFlg && tableFlg && columnFlg) {
							assertEquals(itemInfo, itemInfo2);
							assertEquals(itemInfo2, itemInfo);
						} else {
							assertNotEquals(itemInfo, itemInfo2);
							assertNotEquals(itemInfo2, itemInfo);
						}
						assertEquals(itemInfo.hashCode(), itemInfo.hashCode());
						itemInfo2 = new QueryItemInfo<ItemName>(itemName2, true) {
						};
						assertNotEquals(itemInfo, itemInfo2);
						assertNotEquals(itemInfo2, itemInfo);
					}
				}
			}
		}
	}
}
