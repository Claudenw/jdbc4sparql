package org.xenei.jdbc4sparql.impl.virtual;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.SchemaName;

public class VirtualSchema implements Schema
{
	private final Catalog catalog;
	private final Map<String, VirtualTable> tables;
	private final SchemaName schemaName;

	public VirtualSchema( final Catalog catalog )
	{
		this(catalog, "");
	}

	public VirtualSchema( final Catalog catalog, final String name )
	{
		this.catalog = catalog;
		this.schemaName = new SchemaName(name);
		tables = new HashMap<String, VirtualTable>();
		tables.put("", new VirtualTable(this));
	}

	@Override
	public NameFilter<VirtualTable> findTables( final String tableNamePattern )
	{
		return new NameFilter<VirtualTable>(tableNamePattern, tables.values());
	}

	@Override
	public Catalog getCatalog()
	{
		return catalog;
	}

	@Override
	public SchemaName getName()
	{
		return schemaName;
	}

	@Override
	public VirtualTable getTable( final String tableName )
	{
		return tables.get(tableName);
	}

	@Override
	public Set<? extends VirtualTable> getTables()
	{
		return new HashSet<VirtualTable>(tables.values());
	}

}
