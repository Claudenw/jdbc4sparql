package org.xenei.jdbc4sparql.sparql.items;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;

public class QueryItemCollectionTest {

	private static class TestingNamedObject implements NamedObject<ItemName> {
		private ItemName name;

		public TestingNamedObject(final ItemName name) {
			this.name = name;
		}

		@Override
		public ItemName getName() {
			return name;
		}

		public void setName(final ItemName name) {
			this.name = name;
		}
	}

	private QueryItemCollection<QueryItemInfo<NamedObject<ItemName>, ItemName>, NamedObject<ItemName>, ItemName> itemCollection;

	private NamedObject<ItemName> namedObject;

	@Before
	public void setup() {
		itemCollection = new QueryItemCollection<QueryItemInfo<NamedObject<ItemName>, ItemName>, NamedObject<ItemName>, ItemName>();
		namedObject = new NamedObject<ItemName>() {

			@Override
			public ItemName getName() {
				return new DummyItemName("", "", "", "ItemName", NameSegments.ALL);
			}
		};

		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = null;

		for (final String catalog : new String[] {
				"catalog", "catalog2"
		}) {
			for (final String schema : new String[] {
					"schema", "schema2"
			}) {
				for (final String table : new String[] {
						"table", "table2"
				}) {
					for (final String column : new String[] {
							"column", "column2"
					}) {
						name = new DummyItemName(catalog, schema, table, column, NameSegments.ALL);
						final TestingNamedObject tno = new TestingNamedObject(
								name);
						qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
								tno, name, false);
						itemCollection.add(qii);
					}
				}
			}
		}
	}

	@Test
	public void testAdd() {
		final int size = itemCollection.size();

		ItemName name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
				namedObject, name, false);
		assertFalse(itemCollection.add(qii));
		assertEquals(size, itemCollection.size());

		// optional does not matter
		qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(namedObject,
				name, true);
		assertFalse(itemCollection.add(qii));
		assertEquals(size, itemCollection.size());

		name = new DummyItemName("catalog", "schema", "table", "column3", NameSegments.ALL);
		qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(namedObject,
				name, false);
		assertTrue(itemCollection.add(qii));
		assertEquals(size + 1, itemCollection.size());
	}

	@Test
	public void testAddAll() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = null;
		final List<QueryItemInfo<NamedObject<ItemName>, ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>, ItemName>>();

		for (final String catalog : new String[] {
				"catalog3", "catalog4"
		}) {
			for (final String schema : new String[] {
					"schema3", "schema4"
			}) {
				for (final String table : new String[] {
						"table3", "table4"
				}) {
					for (final String column : new String[] {
							"column3", "column4"
					}) {
						name = new DummyItemName(catalog, schema, table, column, NameSegments.ALL);
						qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
								namedObject, name, false);
						lst.add(qii);
					}
				}
			}
		}

		final int size = itemCollection.size();
		final int size2 = lst.size();
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
		ItemName name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		assertTrue(itemCollection.contains(name));
		name = new DummyItemName("catalog", "schema", "table", "column4", NameSegments.ALL);
		assertFalse(itemCollection.contains(name));
		// test that null is wild card
		name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		name.setUsedSegments(NameSegments.TTFT);
		assertTrue(itemCollection.contains(name));
		name = new DummyItemName("catalog", "schema", "table", "column4", NameSegments.ALL);
		name.setUsedSegments(NameSegments.TTFT);
		assertFalse(itemCollection.contains(name));
	}

	@Test
	public void testContains_NamedObject() {

		ItemName name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		final TestingNamedObject testNamedObject = new TestingNamedObject(name);
		assertTrue(itemCollection.contains(testNamedObject));

		testNamedObject.setName(new DummyItemName("catalog", "schema", "table",
				"column4", NameSegments.ALL));
		assertFalse(itemCollection.contains(testNamedObject));

		// test that null is wild card
		name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		name.setUsedSegments(NameSegments.TTFT);
		testNamedObject.setName(name);
		assertTrue(itemCollection.contains(testNamedObject));

		name = new DummyItemName("catalog", "schema", "table", "column4", NameSegments.ALL);
		name.setUsedSegments(NameSegments.TTFT);
		testNamedObject.setName(name);
		assertFalse(itemCollection.contains(testNamedObject));
	}

	@Test
	public void testContainsAll() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = null;
		final List<QueryItemInfo<NamedObject<ItemName>, ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>, ItemName>>();

		for (final String catalog : new String[] {
				"catalog", "catalog2"
		}) {
			for (final String schema : new String[] {
					"schema", "schema2"
			}) {
				for (final String table : new String[] {
						"table", "table2"
				}) {

					name = new DummyItemName(catalog, schema, table, "column", NameSegments.ALL);
					qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
							namedObject, name, false);
					lst.add(qii);
				}
			}
		}

		assertTrue(itemCollection.containsAll(lst));
		name = new DummyItemName("catalog3", "schema", "table", "column", NameSegments.ALL);
		qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(namedObject,
				name, false);
		lst.add(qii);
		assertFalse(itemCollection.containsAll(lst));
	}

	@Test
	public void testGet_int() {
		final ItemName name = new DummyItemName("catalog", "schema", "table",
				"column", NameSegments.ALL);
		final QueryItemInfo<NamedObject<ItemName>, ItemName> qii = itemCollection
				.get(0);
		assertNotNull(qii);
		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName()));

		try {
			itemCollection.get(itemCollection.size());
			fail("Should have thrown IndexOutOfBoundsException");
		} catch (final IndexOutOfBoundsException expected) {
			// do nothing
		}
	}

	@Test
	public void testGet_ItemName() {
		ItemName name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		final QueryItemInfo<NamedObject<ItemName>, ItemName> qii = itemCollection
				.get(name, NameSegments.ALL);
		assertNotNull(qii);
		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName()));

		name = new DummyItemName("catalog", "schema", null, "column", NameSegments.TTFT);
		try {
			itemCollection.get(name, NameSegments.TTFT);
			fail("Should have thrown IllegalArgumentException");
		} catch (final IllegalArgumentException expected) {
			// do nothing;
		}

		name = new DummyItemName("catalog", "schema", "table3", "column", NameSegments.ALL);
		assertNull(itemCollection.get(name, NameSegments.ALL));
	}

	@Test
	public void testIndexOf_ItemName() {
		ItemName name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		int i = itemCollection.indexOf(name, NameSegments.ALL);
		assertEquals(0, i);

		name = new DummyItemName("catalog2", "schema2", "table2", "column2", NameSegments.ALL);
		i = itemCollection.indexOf(name, NameSegments.ALL);
		assertEquals(15, i);

		name = new DummyItemName("catalog3", "schema2", "table2", "column2", NameSegments.ALL);
		i = itemCollection.indexOf(name, NameSegments.ALL);
		assertEquals(-1, i);

	}

	@Test
	public void testMatch_ItemName() {
		ItemName name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);

		Iterator<QueryItemInfo<NamedObject<ItemName>, ItemName>> iter = itemCollection
				.match(name, NameSegments.ALL);
		assertTrue(iter.hasNext());
		final QueryItemInfo<NamedObject<ItemName>, ItemName> qii = iter.next();
		assertNotNull(qii);
		assertFalse(iter.hasNext());
		assertEquals(0, ItemName.COMPARATOR.compare(name, qii.getName()));

		name = new DummyItemName("catalog", "schema", "table", null, NameSegments.TTTF);
		iter = itemCollection.match(name, name.getUsedSegments());
		assertTrue(iter.hasNext());
		iter.next();
		assertTrue(iter.hasNext());
		iter.next();
		assertFalse(iter.hasNext());
	}

	@Test
	public void testNotMatch_ItemName() {
		final ItemName name = new DummyItemName("catalog", "schema", "table",
				"column", NameSegments.ALL);

		final Iterator<QueryItemInfo<NamedObject<ItemName>, ItemName>> iter = itemCollection
				.notMatch(name, name.getUsedSegments());

		assertTrue(iter.hasNext());
		while (iter.hasNext()) {
			final QueryItemInfo<NamedObject<ItemName>, ItemName> qii = iter
					.next();
			final int i = ItemName.COMPARATOR.compare(name, qii.getName());
			assertTrue(i != 0);
		}
	}

	@Test
	public void testRemove() {
		ItemName name = new DummyItemName("catalog3", "schema", "table", "column", NameSegments.ALL);
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
				namedObject, name, false);
		assertFalse(itemCollection.remove(qii));

		final int size = itemCollection.size();
		name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(namedObject,
				name, false);
		assertTrue(itemCollection.remove(qii));
		assertEquals(size - 1, itemCollection.size());
	}

	@Test
	public void testRemoveAll() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = null;
		final List<QueryItemInfo<NamedObject<ItemName>, ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>, ItemName>>();

		for (final String catalog : new String[] {
				"catalog", "catalog2"
		}) {
			for (final String schema : new String[] {
					"schema", "schema2"
			}) {
				for (final String table : new String[] {
						"table", "table2"
				}) {

					name = new DummyItemName(catalog, schema, table, "column", NameSegments.ALL);
					qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
							namedObject, name, false);
					lst.add(qii);
				}
			}
		}

		final int startSize = lst.size();
		final int size = itemCollection.size();

		assertTrue(itemCollection.removeAll(lst));
		assertEquals(size - startSize, itemCollection.size());
	}

	@Test
	public void testRemoveAllWithTooMany() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = null;
		final List<QueryItemInfo<NamedObject<ItemName>, ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>, ItemName>>();

		for (final String catalog : new String[] {
				"catalog", "catalog2"
		}) {
			for (final String schema : new String[] {
					"schema", "schema2"
			}) {
				for (final String table : new String[] {
						"table", "table2"
				}) {

					name = new DummyItemName(catalog, schema, table, "column", NameSegments.ALL);
					qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
							namedObject, name, false);
					lst.add(qii);
				}
			}
		}
		final int startSize = lst.size();
		name = new DummyItemName("catalog4", "schema4", "table4", "column", NameSegments.ALL);
		qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(namedObject,
				name, false);
		lst.add(qii);

		final int size = itemCollection.size();

		// true == collection was changed
		assertTrue(itemCollection.removeAll(lst));
		assertEquals(size - startSize, itemCollection.size());
	}

	@Test
	public void testRetainAll() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = null;
		final List<QueryItemInfo<NamedObject<ItemName>, ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>, ItemName>>();

		for (final String catalog : new String[] {
				"catalog", "catalog2"
		}) {
			for (final String schema : new String[] {
					"schema", "schema2"
			}) {
				for (final String table : new String[] {
						"table", "table2"
				}) {

					name = new DummyItemName(catalog, schema, table, "column", NameSegments.ALL);
					qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
							namedObject, name, false);
					lst.add(qii);
				}
			}
		}

		assertTrue(itemCollection.retainAll(lst));
		assertEquals(lst.size(), itemCollection.size());
	}

	@Test
	public void testRetainAll_ItemName() {
		final List<ItemName> lst = new ArrayList<ItemName>();

		for (final String catalog : new String[] {
				"catalog", "catalog2"
		}) {
			for (final String schema : new String[] {
					"schema", "schema2"
			}) {
				for (final String table : new String[] {
						"table", "table2"
				}) {

					lst.add(new DummyItemName(catalog, schema, table, "column", NameSegments.ALL));
				}
			}
		}

		assertTrue(itemCollection.retainAll(lst));
		assertEquals(lst.size(), itemCollection.size());
	}

	@Test
	public void testRetainAll_NamedObject() {
		ItemName name = null;
		final List<NamedObject<ItemName>> lst = new ArrayList<NamedObject<ItemName>>();

		for (final String catalog : new String[] {
				"catalog", "catalog2"
		}) {
			for (final String schema : new String[] {
					"schema", "schema2"
			}) {
				for (final String table : new String[] {
						"table", "table2"
				}) {

					name = new DummyItemName(catalog, schema, table, "column", NameSegments.ALL);
					lst.add(new TestingNamedObject(name));
				}
			}
		}

		assertTrue(itemCollection.retainAll(lst));
		assertEquals(lst.size(), itemCollection.size());
	}

	@Test
	public void testRetainAllWithMore() {
		ItemName name = null;
		QueryItemInfo<NamedObject<ItemName>, ItemName> qii = null;
		final List<QueryItemInfo<NamedObject<ItemName>, ItemName>> lst = new ArrayList<QueryItemInfo<NamedObject<ItemName>, ItemName>>();

		for (final String catalog : new String[] {
				"catalog", "catalog2"
		}) {
			for (final String schema : new String[] {
					"schema", "schema2"
			}) {
				for (final String table : new String[] {
						"table", "table2"
				}) {

					name = new DummyItemName(catalog, schema, table, "column", NameSegments.ALL);
					qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(
							namedObject, name, false);
					lst.add(qii);
				}
			}
		}

		name = new DummyItemName("catalog4", "schema4", "table4", "column", NameSegments.ALL);
		qii = new QueryItemInfo<NamedObject<ItemName>, ItemName>(namedObject,
				name, false);
		lst.add(qii);

		assertTrue(itemCollection.retainAll(lst));
		assertEquals(lst.size() - 1, itemCollection.size());
	}

	@Test
	public void testToArray() {
		final Object[] ary = itemCollection.toArray();
		assertNotNull(ary);
		assertEquals(itemCollection.size(), ary.length);
	}

	@Test
	public void testToArrayWithType() {
		QueryItemInfo<?, ?>[] ary = new QueryItemInfo[itemCollection.size()];
		Object[] ary2 = itemCollection.toArray(ary);
		assertTrue(ary2 == ary);

		ary = new QueryItemInfo<?, ?>[0];
		ary2 = itemCollection.toArray(ary);
		assertTrue(ary2 != ary);
		assertEquals(itemCollection.size(), ary2.length);
	}

	@Test
	public void testFindGUID_String() {
		ItemName name = new DummyItemName("catalog", "schema", "table", "column", NameSegments.ALL);
		QueryItemInfo<?, ?> itemInfo = itemCollection.findGUID(name);
		assertNotNull(itemInfo);
		assertEquals(name, itemInfo.getName());

		name = new DummyItemName("catalog", "schema", "table", "column5", NameSegments.ALL);
		itemInfo = itemCollection.findGUID(name);
		assertNull(itemInfo);
	}

}
