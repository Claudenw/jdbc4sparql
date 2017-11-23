package org.xenei.jdbc4sparql.impl.virtual;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.SchemaName;

/**
 * The Virtual schema for all virtual tables.
 *
 */
public class VirtualSchema implements Schema {
	public static final String NAME = "";
	private final Catalog catalog;
	private final Map<String, Table> tables;
	private final SchemaName schemaName;
	
	/**
	 * @return the default virtual schema name.
	 */
	public static SchemaName getDefaultName() {
		return VirtualCatalog.getDefaultName().getSchemaName(NAME);
	}

	public VirtualSchema(final Catalog catalog) {
		this(catalog, NAME);
	}

	public VirtualSchema(final Catalog catalog, final String name) {
		this.catalog = catalog;
		this.schemaName = catalog.getName().getSchemaName(name);
		tables = new HashMap<String, Table>();
		tables.put(VirtualTable.NAME, new VirtualTable(this));
		tables.put(VirtualTable.SYSTEM_TABLE, new VirtualTable(this,
				VirtualTable.SYSTEM_TABLE));
	}

	@Override
	public NameFilter<Table> findTables(final String tableNamePattern) {
		return new NameFilter<Table>(tableNamePattern, tables.values());
	}

	@Override
	public Catalog getCatalog() {
		return catalog;
	}

	@Override
	public SchemaName getName() {
		return schemaName;
	}

	@Override
	public Table getTable(final String tableName) {
		return tables.get(tableName);
	}

	@Override
	public Set<Table> getTables() {
		return new HashSet<Table>(tables.values());
	}

}
