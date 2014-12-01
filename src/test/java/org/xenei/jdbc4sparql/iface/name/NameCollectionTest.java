package org.xenei.jdbc4sparql.iface.name;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;

@Ignore
public class NameCollectionTest {

	private NameCollection<ItemName> collection;
	private ItemName itemName;

	@Before
	public void setup() {
		collection = new NameCollection<ItemName>();
		itemName = new SearchName("catalog", "schema", "table", "column");

		Boolean vals[] = new Boolean[] { Boolean.TRUE, Boolean.FALSE };
		List<Boolean[]> lst = new ArrayList<Boolean[]>();
		for (Boolean catalog : vals) {
			for (Boolean schema : vals) {
				for (Boolean table : vals) {
					for (Boolean column : vals) {
						itemName.setUsedSegments(new NameSegments(catalog,
								schema, table, column));
						collection.add(itemName);
					}
				}
			}

		}

	}

}
