package org.xenei.jdbc4sparql.impl;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;


import org.apache.commons.collections.bag.TreeBag;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.SortKey;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.meta.FixedResultSet;
import org.xenei.jdbc4sparql.meta.MetaNamespace;

public abstract class AbstractTable extends MetaNamespace implements Table
{
	private TableDef tableDef;
	private Schema schema;
	
	@SuppressWarnings( "unchecked" )
	public 	AbstractTable( Schema schema, TableDef tableDef )
	{
		this.schema = schema;
		this.tableDef= tableDef;
	}
	
	public String toString()
	{
		return String.format( "Table[ %s.%s ]", getCatalog().getLocalName(), getDBName() );
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
	
	@Override
	public String getDBName()
	{
		return String.format( "%s.%s", schema.getLocalName(), getLocalName() );
	}
	
	abstract public ResultSet getResultSet();

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

	public String getName()
	{
		return tableDef.getName();
	}

	public List<? extends ColumnDef> getColumnDefs()
	{
		return tableDef.getColumnDefs();
	}

	public ColumnDef getColumnDef( int idx )
	{
		return tableDef.getColumnDef(idx);
	}

	public ColumnDef getColumnDef( String name )
	{
		return tableDef.getColumnDef(name);
	}

	public int getColumnCount()
	{
		return tableDef.getColumnCount();
	}

	public SortKey getSortKey()
	{
		return tableDef.getSortKey();
	}

	public void verify( Object[] row )
	{
		tableDef.verify(row);
	}

	public int getColumnIndex( ColumnDef column )
	{
		return tableDef.getColumnIndex(column);
	}

	public int getColumnIndex( String columnName )
	{
		return tableDef.getColumnIndex(columnName);
	}

	@Override
	public Iterator<Column> getColumns()
	{
		return new Table.ColumnIterator(this, getColumnDefs());
	}

	@Override
	public Column getColumn( int idx )
	{
		return new ColumnImpl( this, getColumnDef(idx));
	}

	@Override
	public Column getColumn( String name )
	{
		return new ColumnImpl( this, getColumnDef(name));
	}

	@Override
	public NameFilter<Column> findColumns( String columnNamePattern )
	{
		return new NameFilter<Column>( columnNamePattern, getColumns());
	}
	
}
