package org.xenei.jdbc4sparql.sparql.items;

import org.xenei.jdbc4sparql.iface.name.ItemName;

public interface NamedObject<T extends ItemName> {
	public T getName();
}
