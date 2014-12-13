package org.xenei.jdbc4sparql.sparql.items;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.ItemNameTest;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
//import org.xenei.jdbc4sparql.iface.name.RenamedBaseName;
import org.xenei.jdbc4sparql.iface.name.SearchName;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.iterator.Filter;

public class QueryItemCollectionTest {

	private QueryItemCollection<QueryItemInfo<NamedObject<ItemName>, ItemName>, NamedObject<ItemName>, ItemName> itemCollection;
	private NamedObject<ItemName> namedObject;
	
	@Before
	public void setup() {
		itemCollection = new QueryItemCollection<QueryItemInfo<NamedObject<ItemName>, ItemName>, NamedObject<ItemName>, ItemName>();
		namedObject = new NamedObject<ItemName>(){

			@Override
			public ItemName getName() {
				return new SearchName("","","","ItemName");
			}};
			
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = null;

		for (String catalog : new String[] { "catalog", "catalog2" }) {
			for (String schema : new String[] { "schema", "schema2" }) {
				for (String table : new String[] { "table", "table2" }) {
					for (String column : new String[] { "column", "column2" }) {
						name = new SearchName(catalog, schema,
								table, column);
						qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject, name, false);
						itemCollection.add(qii);
					}
				}
			}
		}
	}

	@Test
	public void testAdd() {
		int size = itemCollection.size();

		ItemName name = new SearchName("catalog", "schema", "table",
				"column");
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		assertFalse(itemCollection.add(qii));
		assertEquals(size, itemCollection.size());

		// optional does not matter
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, true);
		assertFalse(itemCollection.add(qii));
		assertEquals(size, itemCollection.size());

		name = new SearchName("catalog", "schema", "table",
				"column3");
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		assertTrue(itemCollection.add(qii));
		assertEquals(size + 1, itemCollection.size());
	}

	@Test
	public void testAddAll() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = null;
		List<QueryItemInfo<NamedObject<ItemName>,ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>,ItemName>>();

		for (String catalog : new String[] { "catalog3", "catalog4" }) {
			for (String schema : new String[] { "schema3", "schema4" }) {
				for (String table : new String[] { "table3", "table4" }) {
					for (String column : new String[] { "column3", "column4" }) {
						name = new SearchName(catalog, schema,
								table, column);
						qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
						lst.add(qii);
					}
				}
			}
		}

		int size = itemCollection.size();
		int size2 = lst.size();
		assertTrue(itemCollection.addAll(lst));
		assertEquals(size + size2, itemCollection.size());
	}

	@Test
	public void testClear() {
		assertTrue(itemCollection.size() > 0);
		assertFalse(itemCollection.isEmpty());
		itemCollection.clear();
		assertEquals(0, itemCollection.size());
		assertTrue(itemCollection.isEmpty());
	}

	@Test
	public void testContains_ItemName() {
		ItemName name = new SearchName("catalog", "schema", "table",
				"column");
		assertTrue(itemCollection.contains(name));
		name = new SearchName("catalog", "schema", "table",
				"column4");
		assertFalse(itemCollection.contains(name));
		// test that null is wild card
		name = new SearchName("catalog", "schema", "table", "column");
		name.setUsedSegments( NameSegments.TTFT);
		assertTrue(itemCollection.contains(name));
		name = new SearchName("catalog", "schema", "table", "column4");
		name.setUsedSegments( NameSegments.TTFT);
		assertFalse(itemCollection.contains(name));
	}

	@Test
	public void testContains_NamedObject() {
		ItemName name = new SearchName("catalog", "schema", "table",
				"column");
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		assertTrue(itemCollection.contains(qii));
		name = new SearchName("catalog", "schema", "table",
				"column4");
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		assertFalse(itemCollection.contains(qii));

		// test that null is wild card
		name = new SearchName("catalog", "schema", "table", "column");
		name.setUsedSegments( NameSegments.TTFT);
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		assertTrue(itemCollection.contains(qii));
		name = new SearchName("catalog", "schema", "table", "column4");
		name.setUsedSegments( NameSegments.TTFT);
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		assertFalse(itemCollection.contains(qii));
	}

	@Test
	public void testContainsAll() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = null;
		List<QueryItemInfo<NamedObject<ItemName>,ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>,ItemName>>();

		for (String catalog : new String[] { "catalog", "catalog2" }) {
			for (String schema : new String[] { "schema", "schema2" }) {
				for (String table : new String[] { "table", "table2" }) {

					name = new SearchName(catalog, schema, table,
							"column");
					qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
					lst.add(qii);
				}
			}
		}

		assertTrue(itemCollection.containsAll(lst));
		name = new SearchName("catalog3", "schema", "table",
				"column");
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		lst.add(qii);
		assertFalse(itemCollection.containsAll(lst));
	}

	@Test
	public void testRemove() {
		ItemName name = new SearchName("catalog3", "schema",
				"table", "column");
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		assertFalse(itemCollection.remove(qii));

		int size = itemCollection.size();
		name = new SearchName("catalog", "schema", "table", "column");
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		assertTrue(itemCollection.remove(qii));
		assertEquals(size - 1, itemCollection.size());
	}

	@Test
	public void testRemoveAll() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = null;
		List<QueryItemInfo<NamedObject<ItemName>,ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>,ItemName>>();

		for (String catalog : new String[] { "catalog", "catalog2" }) {
			for (String schema : new String[] { "schema", "schema2" }) {
				for (String table : new String[] { "table", "table2" }) {

					name = new SearchName(catalog, schema, table,
							"column");
					qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
					lst.add(qii);
				}
			}
		}

		int startSize = lst.size();
		int size = itemCollection.size();

		assertTrue(itemCollection.removeAll(lst));
		assertEquals(size - startSize, itemCollection.size());
	}

	@Test
	public void testRemoveAllWithTooMany() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = null;
		List<QueryItemInfo<NamedObject<ItemName>,ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>,ItemName>>();

		for (String catalog : new String[] { "catalog", "catalog2" }) {
			for (String schema : new String[] { "schema", "schema2" }) {
				for (String table : new String[] { "table", "table2" }) {

					name = new SearchName(catalog, schema, table,
							"column");
					qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
					lst.add(qii);
				}
			}
		}
		int startSize = lst.size();
		name = new SearchName("catalog4", "schema4", "table4",
				"column");
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		lst.add(qii);

		int size = itemCollection.size();

		assertFalse(itemCollection.removeAll(lst));
		assertEquals(size - startSize, itemCollection.size());
	}

	@Test
	public void testRetainAll() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = null;
		List<QueryItemInfo<NamedObject<ItemName>,ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>,ItemName>>();

		for (String catalog : new String[] { "catalog", "catalog2" }) {
			for (String schema : new String[] { "schema", "schema2" }) {
				for (String table : new String[] { "table", "table2" }) {

					name = new SearchName(catalog, schema, table,
							"column");
					qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
					lst.add(qii);
				}
			}
		}

		assertTrue(itemCollection.retainAll(lst));
		assertEquals(lst.size(), itemCollection.size());
	}

	@Test
	public void testRetainAllWithMore() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = null;
		List<QueryItemInfo<NamedObject<ItemName>,ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>,ItemName>>();

		for (String catalog : new String[] { "catalog", "catalog2" }) {
			for (String schema : new String[] { "schema", "schema2" }) {
				for (String table : new String[] { "table", "table2" }) {

					name = new SearchName(catalog, schema, table,
							"column");
					qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
					lst.add(qii);
				}
			}
		}

		name = new SearchName("catalog4", "schema4", "table4",
				"column");
		qii = new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,name, false);
		lst.add(qii);

		assertTrue(itemCollection.retainAll(lst));
		assertEquals(lst.size() - 1, itemCollection.size());
	}

	@Test
	public void testToArray() {
		Object[] ary = itemCollection.toArray();
		assertNotNull(ary);
		assertEquals(itemCollection.size(), ary.length);
	}

	@Test
	public void testToArrayWithType() {
		QueryItemInfo<?,?>[] ary = new QueryItemInfo[itemCollection.size()];
		Object[] ary2 = itemCollection.toArray(ary);
		assertTrue(ary2 == ary);

		ary = new QueryItemInfo<?,?>[0];
		ary2 = itemCollection.toArray(ary);
		assertTrue(ary2 != ary);
		assertEquals(itemCollection.size(), ary2.length);
	}

	@Test
	public void testGet_ItemName() {
		ItemName name = new SearchName("catalog", "schema", "table",
				"column");
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = itemCollection.get(name);
		assertNotNull(qii);
		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName()));

		name = new SearchName("catalog", "schema", null, "column");
		try {
			itemCollection.get(name);
			fail("Should have thrown IllegalArgumentException");
		} catch (IllegalArgumentException expected) {
			// do nothing;
		}

		name = new SearchName("catalog", "schema", "table3",
				"column");
		assertNull(itemCollection.get(name));
	}

	@Test
	public void testGet_int() {
		ItemName name = new SearchName("catalog", "schema", "table",
				"column");
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = itemCollection.get(0);
		assertNotNull(qii);
		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName()));

		try {
			itemCollection.get(itemCollection.size());
			fail("Should have thrown IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException expected) {
			// do nothing
		}
	}

//	@Test
//	public void testFindGUID() {
//		SearchName name = new SearchName("catalog",
//				"schema", "table", "column");
//
//		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = itemCollection.findGUID(name);
//		assertNotNull(qii);
//		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName()));
//
////		RenamedBaseName renamed = name.rename("catalog", "schema", "table",
////				"column3");
////		qii = itemCollection.findGUID(renamed);
////		assertNotNull(qii);
////		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName()));
//
//		name = new SearchName("catalog", "schema", null, "column");
//		assertNull(itemCollection.findGUID(name));
//
//	}

	@Test
	public void testMatch_ItemName() {
		ItemName name = new SearchName("catalog", "schema", "table",
				"column");

		Iterator<QueryItemInfo<NamedObject<ItemName>,ItemName>> iter = itemCollection.match(name);
		assertTrue(iter.hasNext());
		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = iter.next();
		assertNotNull(qii);
		assertFalse(iter.hasNext());
		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName()));

		name = new SearchName("catalog", "schema", "table", null);
		iter = itemCollection.match(name);
		assertTrue(iter.hasNext());
		iter.next();
		assertTrue(iter.hasNext());
		iter.next();
		assertFalse(iter.hasNext());
	}

//	@Test
//	public void testMatch_Collection() {
//		ItemName name = new SearchName("catalog", "schema", "table",
//				"column");
//		ItemName name2 = new SearchName("catalog", "schema",
//				"table", "column2");
//
//		List<ItemName> lst = new ArrayList<ItemName>();
//		lst.add(new SearchName("catalog", "schema", "table",
//				"column"));
//
//		lst.add(new SearchName("catalog", "schema", "table",
//				"column2"));
//
//		Iterator<QueryItemInfo<NamedObject<ItemName>,ItemName>> iter = itemCollection.match(lst);
//		assertTrue(iter.hasNext());
//		QueryItemInfo<NamedObject<ItemName>,ItemName> qii = iter.next();
//		assertNotNull(qii);
//		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName())
//				* ItemName.COMPARATOR.compare(name2, qii.getName()));
//		assertTrue(iter.hasNext());
//		qii = iter.next();
//		assertNotNull(qii);
//		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName())
//				* ItemName.COMPARATOR.compare(name2, qii.getName()));
//		assertFalse(iter.hasNext());
//
//		List<QueryItemInfo<NamedObject<ItemName>,ItemName>> lst2 = new ArrayList<QueryItemInfo<NamedObject<ItemName>,ItemName>>();
//		lst2.add(new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,new SearchName(
//				"catalog", "schema", "table", "column"), false));
//
//		lst2.add(new QueryItemInfo<NamedObject<ItemName>,ItemName>(namedObject,new SearchName(
//				"catalog", "schema", "table", "column2"), false));
//
//		iter = itemCollection.match(lst2);
//		assertTrue(iter.hasNext());
//		qii = iter.next();
//		assertNotNull(qii);
//		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName())
//				* ItemName.COMPARATOR.compare(name2, qii.getName()));
//		assertTrue(iter.hasNext());
//		qii = iter.next();
//		assertNotNull(qii);
//		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName())
//				* ItemName.COMPARATOR.compare(name2, qii.getName()));
//		assertFalse(iter.hasNext());
//	}

	@Test
	public void testNotMatch_ItemName() {
		ItemName name = new SearchName("catalog", "schema", "table",
				"column");

		Iterator<QueryItemInfo<NamedObject<ItemName>,ItemName>> iter = itemCollection.notMatch(name);

		assertTrue(iter.hasNext());
		while (iter.hasNext()) {
			QueryItemInfo<NamedObject<ItemName>,ItemName> qii = iter.next();
			int i = ItemName.COMPARATOR.compare(name, qii.getName());
			assertTrue(i != 0);
		}
	}

//	@Test
//	public void testNotMatch_Collection() {
//		ItemName name = new SearchName("catalog", "schema", "table",
//				"column");
//		ItemName name2 = new SearchName("catalog", "schema",
//				"table", "column2");
//
//		List<ItemName> lst = new ArrayList<ItemName>();
//		lst.add(new SearchName("catalog", "schema", "table",
//				"column"));
//
//		lst.add(new SearchName("catalog", "schema", "table",
//				"column2"));
//
//		Iterator<QueryItemInfo<NamedObject<ItemName>,ItemName>> iter = itemCollection.notMatch(lst);
//		assertTrue(iter.hasNext());
//		while (iter.hasNext()) {
//			QueryItemInfo<NamedObject<ItemName>,ItemName> qii = iter.next();
//			int i = ItemName.COMPARATOR.compare(name, qii.getName())
//					* ItemName.COMPARATOR.compare(name2, qii.getName());
//			assertTrue(0 != i);
//		}
//	}

	@Test
	public void testIndexOf_ItemName() {
		ItemName name = new SearchName("catalog", "schema", "table",
				"column");
		int i = itemCollection.indexOf(name);
		assertEquals(0, i);

		name = new SearchName("catalog2", "schema2", "table2",
				"column2");
		i = itemCollection.indexOf(name);
		assertEquals(15, i);

		name = new SearchName("catalog3", "schema2", "table2",
				"column2");
		i = itemCollection.indexOf(name);
		assertEquals(-1, i);

	}

	
}
