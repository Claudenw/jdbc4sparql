package org.xenei.jdbc4sparql.iface.name;

import com.hp.hpl.jena.util.iterator.Map1;

public class SQLNameMap implements Map1<ItemName, String> {

	public SQLNameMap() {
	}

	@Override
	public String map1(ItemName o) {
		return o.getDBName();
	}

}
