package org.xenei.jdbc4sparql.iface;

import java.util.Collection;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.meta.ColumnsTableRow;

public interface Schema extends NamespacedObject
{
	
	Set<Table> getTables();

	/**
	 * The catalog this schema is in.
	 * @return Catalog
	 */
	Catalog getCatalog();
	
	Table getTable(String tableName);
}
