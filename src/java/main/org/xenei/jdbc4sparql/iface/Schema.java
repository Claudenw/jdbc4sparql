package org.xenei.jdbc4sparql.iface;

import java.util.Set;

public interface Schema extends NamespacedObject
{

	NameFilter<Table> findTables( String tableNamePattern );

	/**
	 * The catalog this schema is in.
	 * 
	 * @return Catalog
	 */
	Catalog getCatalog();

	Table getTable( String tableName );

	Set<Table> getTables();
}
