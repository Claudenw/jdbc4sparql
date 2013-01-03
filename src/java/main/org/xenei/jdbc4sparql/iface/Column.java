package org.xenei.jdbc4sparql.iface;

public interface Column extends NamespacedObject, ColumnDef
{
	Catalog getCatalog();

	String getDBName();

	Schema getSchema();

	Table getTable();
}
