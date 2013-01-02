package org.xenei.jdbc4sparql.impl;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
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
import org.xenei.jdbc4sparql.meta.MetaNamespace;

/**
 * An implementation of AbstractTable that stores the data
 * in a collection of object arrays.
 * 
 * This table is useful for fixed data sets (e.g. schema tables)
 */
public class DataTable extends AbstractTable
{
	private Collection<Object[]>data;

	/**
	 * Constructor that uses the schema namespace for table namespace.
	 * @param schema The schema this table is in
	 * @param tableDef The table definition to use.
	 */
	public 	DataTable( Schema schema, TableDef tableDef )
	{
		this( schema.getNamespace(), schema, tableDef );
	}
	
	/**
	 * Constructor.
	 * @param namespace The namespace for the table
	 * @param schema The schema to use.
	 * @param tableDef The table definition to use
	 */
	@SuppressWarnings( "unchecked" )
	public 	DataTable( String namespace, Schema schema, TableDef tableDef )
	{
		super( schema, tableDef);
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
			{	
				// supress warning is for this conversion as TreeBag is not generic.
				data = new TreeBag( tableDef.getSortKey() );
			}
		}
		
	}
	
	/**
	 * Add an object array as a row in the table.
	 * @throws IllegalArgumentException on data errors.
	 * @param args the data to add.
	 */
	public void addData( Object[] args )
	{
		getTableDef().verify( args );
		((Collection<Object[]>)data).add( args );
	}
	
	/**
	 * Get a result set that iterates over this table.
	 * @return
	 */
	public ResultSet getResultSet() throws SQLException
	{
		ResultSet retval = null;;
		if (data instanceof TreeSet)
		{
			NavigableSet<Object[]> ns = (TreeSet<Object[]>)data;
			retval = new NavigableSetResultSet(ns, this){

				@Override
				protected Object readObject( int idx ) throws SQLException
				{
					checkColumn( idx);
					Object[] rowData = (Object[]) getRowObject();
					return rowData[idx];
				}};
		}
		else if (data instanceof TreeBag)
		{
			retval = new IteratorResultSet( data.iterator(), this ){

				@Override
				protected Object readObject( int idx ) throws SQLException
				{
					Object[] rowData = (Object[]) getRowObject();
					return rowData[idx];
				}};
		}
		else
		{
			retval = new ListResultSet( (List<?>)data, this ){

				@Override
				protected Object readObject( int idx ) throws SQLException
				{
					checkColumn( idx);
					Object[] rowData = (Object[]) getRowObject();
					return rowData[idx];
				}};
		}
		return retval;
		
	}

	public boolean isEmpty()
	{
		return data.isEmpty();
	}

}
