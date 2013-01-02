package org.xenei.jdbc4sparql.impl;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public class SchemaImpl extends NamespaceImpl implements Schema 
{
	private Catalog catalog;
	private Map<String,Table> tables;
	private static Map<String,TableDef> tableDefs;


	public SchemaImpl( Catalog catalog, String localName )
	{
		this( catalog, catalog.getNamespace(), localName );
	}

	public SchemaImpl( Catalog catalog, String namespace, String localName )
	{
		super(namespace, localName);
		this.catalog = catalog;
		this.tables = new HashMap<String,Table>();
		this.tableDefs = new HashMap<String,TableDef>();
	}

	@Override
	public Catalog getCatalog()
	{
		return catalog;
	}
	
	public void addTableDef(TableDef tableDef)
	{
		tableDefs.put( tableDef.getName(),  tableDef );
	}
	
	public void addTableDefs(Collection<TableDef> tableDefs)
	{
		for (TableDef t : tableDefs)
		{
			addTableDef( t );
		}
	}
	
	public TableDef getTableDef(String localName )
	{
		return tableDefs.get( localName );
	}
	
	/** Returns a table with no data
	 * 
	 * @param name
	 * @return
	 */
	public Table newTable( String name )
	{
		TableDef tableDef = getTableDef(name);
		if (tableDef == null)
		{
			throw new IllegalArgumentException( name+" is not a table in this schema");
		}
		return new DataTable( this, tableDef );	
	}
	
	public Set<Table> getTables()
	{
		HashSet<Table> retval = new HashSet<Table>(tables.values());
		for (String tableName : tableDefs.keySet())
		{
			if (!tables.containsKey(tableName))
			{
				retval.add( new DataTable( this, tableDefs.get(tableName)));
			}
		}
		return retval;
	}
	
	public Table getTable( String name ) 
	{	
		Table retval = null;
		if (tables.get(name) == null)
		{
			retval = newTable( name );
			tables.put( name,  retval );
		}
		return retval;
	}
	
	public void addTable( Table table )
	{
		for (Schema schema : getCatalog().findSchemas(null))
		{
			if (!schema.getLocalName().equals( getLocalName()))
			{
				for (Table tbl : schema.findTables( table.getLocalName()))
				{
					if (tbl.getFQName().equals(table.getFQName()))
					{
						throw new IllegalArgumentException( String.format( "Name conflict tables %s and %s have FQName of %s",
								table.getDBName(), tbl.getDBName(), table.getFQName() ));
					}
				}
			}
		}
		tables.put( table.getLocalName(), table );
	}
	
	@Override
	public NameFilter<Table> findTables( String tableNamePattern )
	{
		return new NameFilter<Table>( tableNamePattern, getTables());
	}
}
