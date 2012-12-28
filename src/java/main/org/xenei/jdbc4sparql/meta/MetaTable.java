package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;


import org.apache.commons.collections.bag.TreeBag;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public class MetaTable extends MetaNamespace implements Table
{
	private TableDef tableDef;
	private Collection<Object[]>data;
	private String name;
	private Schema schema;
	
	@SuppressWarnings( "unchecked" )
	MetaTable( Schema schema, TableDef tableDef )
	{
		this.schema = schema;
		this.tableDef= tableDef;
		if (tableDef.getSortKey() == null)
		{
			data = new ArrayList<Object[]>();
		}
		else
		{
			if (tableDef.getSortKey().isUnique())
			{
				data = new TreeSet<Object[]>( tableDef.getSortKey());
			}
			else
			{	// 11 is the default priority queue capacity
				data = new TreeBag( tableDef.getSortKey() );
			}
		}
	}
	
	public void addData( Object[] args )
	{
		tableDef.verify( args );
		data.add( args );
	}

	@Override
	public String getLocalName()
	{
		return tableDef.getName();
	}

	@Override
	public Schema getSchema()
	{
		return schema;
	}
	
	public ResultSet getResultSet()
	{
		return new FixedResultSet( data, tableDef );
	}

	@Override
	public Catalog getCatalog()
	{
		return schema.getCatalog();
	}

	public TableDef getTableDef()
	{
		return tableDef;
	}
	
	public String getType()
	{
		return "TABLE";
	}

	@Override
	public boolean isEmpty()
	{
		return data.isEmpty();
	}

}
