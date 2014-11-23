package org.xenei.jdbc4sparql.iface.name;

import com.hp.hpl.jena.util.iterator.Map1;

public class SparqlNameMap implements Map1<ItemName, String> {

	public SparqlNameMap() {
	}

	@Override
	public String map1(ItemName o) {
		return o.getSPARQLName();
	}

}
