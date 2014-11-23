package org.xenei.jdbc4sparql.sparql.items;

import org.xenei.jdbc4sparql.iface.name.ItemName;

import com.hp.hpl.jena.util.iterator.Map1;

public class NamedObjectNameMap<T extends ItemName> implements
		Map1<QueryItemInfo<T>, T> {

	@Override
	public T map1(QueryItemInfo<T> o) {
		return o.getName();
	}

}
