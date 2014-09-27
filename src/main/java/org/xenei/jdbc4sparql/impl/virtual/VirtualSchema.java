package org.xenei.jdbc4sparql.impl.virtual;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.SchemaName;

public class VirtualSchema implements Schema {
	private Catalog catalog;
	private Map<String, VirtualTable> tables;
	private static SchemaName schemaName = new SchemaName( "" );

	public VirtualSchema(Catalog catalog) {
		this.catalog = catalog;
		tables = new HashMap<String,VirtualTable>();
		
	}

	@Override
	public SchemaName getName() {
		return schemaName;
	}

	@Override
	public NameFilter<VirtualTable> findTables(String tableNamePattern) {
		return new NameFilter<VirtualTable>(tableNamePattern, tables.values());
	}

	@Override
	public Catalog getCatalog() {
		return catalog;
	}

	@Override
	public VirtualTable getTable(String tableName) {
		return tables.get(tableName);
	}

	@Override
	public Set<? extends VirtualTable> getTables() {
		return new HashSet<VirtualTable>(tables.values());
	}

}
