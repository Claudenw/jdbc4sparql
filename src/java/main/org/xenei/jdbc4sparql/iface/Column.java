package org.xenei.jdbc4sparql.iface;

public interface Column extends NamespacedObject, ColumnDef
{
	Catalog getCatalog();
	Schema getSchema();
	Table getTable();
}
