package org.xenei.jdbc4sparql.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public class SchemaImpl extends NamespaceImpl implements Schema
{
	private final Catalog catalog;
	private final Map<String, Table> tables;
	private Map<String, TableDef> tableDefs;

	public SchemaImpl( final Catalog catalog, final String localName )
	{
		this(catalog, catalog.getNamespace(), localName);
	}

	public SchemaImpl( final Catalog catalog, final String namespace,
			final String localName )
	{
		super(namespace, localName);
		this.catalog = catalog;
		this.tables = new HashMap<String, Table>();
		tableDefs = new HashMap<String, TableDef>();
	}

	public void addTable( final Table table )
	{
		for (final Schema schema : getCatalog().findSchemas(null))
		{
			if (!schema.getLocalName().equals(getLocalName()))
			{
				for (final Table tbl : schema.findTables(table.getLocalName()))
				{
					if (tbl.getFQName().equals(table.getFQName()))
					{
						throw new IllegalArgumentException(
								String.format(
										"Name conflict tables %s and %s have FQName of %s",
										table.getDBName(), tbl.getDBName(),
										table.getFQName()));
					}
				}
			}
		}
		tables.put(table.getLocalName(), table);
	}

	public void addTableDef( final TableDef tableDef )
	{
		tableDefs.put(tableDef.getLocalName(), tableDef);
	}

	public void addTableDefs( final Collection<TableDef> tableDefs )
	{
		for (final TableDef t : tableDefs)
		{
			addTableDef(t);
		}
	}

	@Override
	public NameFilter<Table> findTables( final String tableNamePattern )
	{
		return new NameFilter<Table>(tableNamePattern, getTables());
	}

	@Override
	public Catalog getCatalog()
	{
		return catalog;
	}

	@Override
	public Table getTable( final String name )
	{
		Table retval = null;
		if (tables.get(name) == null)
		{
			retval = newTable(name);
			tables.put(name, retval);
		}
		return retval;
	}

	public TableDef getTableDef( final String localName )
	{
		return tableDefs.get(localName);
	}

	@Override
	public Set<Table> getTables()
	{
		final HashSet<Table> retval = new HashSet<Table>(tables.values());
		for (final String tableName : tableDefs.keySet())
		{
			if (!tables.containsKey(tableName))
			{
				retval.add(newTable(tableName));
			}
		}
		return retval;
	}

	/**
	 * Returns a table with no data
	 * 
	 * @param name
	 * @return
	 */
	public Table newTable( final String name )
	{
		final TableDef tableDef = getTableDef(name);
		if (tableDef == null)
		{
			throw new IllegalArgumentException(name
					+ " is not a table in this schema");
		}
		return new DataTable(this, tableDef);
	}
}
