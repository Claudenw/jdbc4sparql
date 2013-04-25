/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

/**
 * A schema implementation.
 */
public class SchemaImpl extends NamespaceImpl implements Schema
{
	// The catalog the schema is in.
	private final Catalog catalog;
	// a map of table local names to tables in the catalog.
	private final Map<String, Table> tables;
	// a map of tabledef local names to tabledefs in the catalog.
	private final Map<String, TableDef> tableDefs;

	/**
	 * Construcor.
	 * 
	 * Uses catalog namespace.
	 * 
	 * @param catalog
	 *            The catalog the schema is in.
	 * @param localName
	 *            The local name for the schema.
	 */
	public SchemaImpl( final Catalog catalog, final String localName )
	{
		this(catalog, catalog.getNamespace(), localName);
	}

	/**
	 * Constructor
	 * 
	 * @param catalog
	 *            The catalog the schema is in.
	 * @param namespace
	 *            The namespace the catalog is in.
	 * @param localName
	 *            The local name for the schema.
	 */
	public SchemaImpl( final Catalog catalog, final String namespace,
			final String localName )
	{
		super(namespace, localName);
		this.catalog = catalog;
		this.tables = new HashMap<String, Table>();
		tableDefs = new HashMap<String, TableDef>();
	}

	/**
	 * Add a table to the schema.
	 * 
	 * @param table
	 *            The table to add
	 * @throw IllegalArgumentException if a table wit the same FQName already
	 *        exists in the schema.
	 */
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
										table.getSQLName(), tbl.getSQLName(),
										table.getFQName()));
					}
				}
			}
		}
		tables.put(table.getLocalName(), table);
	}

	/**
	 * Add a table definition to the schema.
	 * 
	 * Will overwrite an existing table definition with the same local name.
	 * 
	 * @param tableDef
	 *            The tabledefinition to add.
	 */
	public void addTableDef( final TableDef tableDef )
	{
		tableDefs.put(tableDef.getLocalName(), tableDef);
	}

	/**
	 * Add a collection of table defs.
	 * 
	 * Will overwrite an existing table definition with the same local name.
	 * 
	 * @param tableDefs
	 */
	public void addTableDefs( final Collection<? extends TableDef> tableDefs )
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

	/**
	 * Get teh table def for the local name.
	 * 
	 * @param localName
	 *            The local name to find
	 * @return The TableDef or null if it is not found.
	 */
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
