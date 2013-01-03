package org.xenei.jdbc4sparql.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.TableDef;

/**
 * An implementation of AbstractTable that stores the data
 * in a collection of object arrays.
 * 
 * This table is useful for fixed data sets (e.g. schema tables)
 */
public class DataTable extends AbstractTable
{
	private Collection<Object[]> data;

	/**
	 * Constructor that uses the schema namespace for table namespace.
	 * 
	 * @param schema
	 *            The schema this table is in
	 * @param tableDef
	 *            The table definition to use.
	 */
	public DataTable( final Schema schema, final TableDef tableDef )
	{
		this(schema.getNamespace(), schema, tableDef);
	}

	/**
	 * Constructor.
	 * 
	 * @param namespace
	 *            The namespace for the table
	 * @param schema
	 *            The schema to use.
	 * @param tableDef
	 *            The table definition to use
	 */
	@SuppressWarnings( "unchecked" )
	public DataTable( final String namespace, final Schema schema,
			final TableDef tableDef )
	{
		super(schema, tableDef);
		if (tableDef.getSortKey() == null)
		{
			data = new ArrayList<Object[]>();
		}
		else
		{
			if (tableDef.getSortKey().isUnique())
			{
				data = new TreeSet<Object[]>(tableDef.getSortKey());
			}
			else
			{
				// supress warning is for this conversion as TreeBag is not
				// generic.
				data = new SortedBag<Object[]>(tableDef.getSortKey());
			}
		}

	}

	/**
	 * Add an object array as a row in the table.
	 * 
	 * @throws IllegalArgumentException
	 *             on data errors.
	 * @param args
	 *            the data to add.
	 */
	public void addData( final Object[] args )
	{
		getTableDef().verify(args);
		data.add(args);
	}

	/**
	 * Get a result set that iterates over this table.
	 * 
	 * @return
	 */
	@Override
	public ResultSet getResultSet() throws SQLException
	{
		ResultSet retval = null;
		;
		if (data instanceof TreeSet)
		{
			final NavigableSet<Object[]> ns = (TreeSet<Object[]>) data;
			retval = new NavigableSetResultSet(ns, this) {

				@Override
				protected Object readObject( final int columnOrdinal )
						throws SQLException
				{
					checkColumn(columnOrdinal);
					final Object[] rowData = (Object[]) getRowObject();
					return rowData[columnOrdinal - 1];
				}
			};
		}
		else if (data instanceof SortedBag)
		{
			retval = new IteratorResultSet(data.iterator(), this) {

				@Override
				protected Object readObject( final int columnOrdinal )
						throws SQLException
				{
					checkColumn(columnOrdinal);
					final Object[] rowData = (Object[]) getRowObject();
					return rowData[columnOrdinal - 1];
				}
			};
		}
		else
		{
			retval = new ListResultSet((List<?>) data, this) {

				@Override
				protected Object readObject( final int columnOrdinal )
						throws SQLException
				{
					checkColumn(columnOrdinal);
					final Object[] rowData = (Object[]) getRowObject();
					return rowData[columnOrdinal - 1];
				}
			};
		}
		return retval;

	}

	public boolean isEmpty()
	{
		return data.isEmpty();
	}

}
