package org.xenei.jdbc4sparql.iface;

import java.util.Set;

public interface Schema extends NamespacedObject
{
	
	Set<Table> getTables();

	/**
	 * The catalog this schema is in.
	 * @return Catalog
	 */
	Catalog getCatalog();
}
