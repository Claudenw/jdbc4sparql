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

import com.hp.hpl.jena.rdf.model.Resource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.Table;

/**
 * An implementation of AbstractTable that stores the data
 * in a collection of object arrays.
 * 
 * This table is useful for fixed data sets (e.g. schema tables)
 */
public class DataTable extends AbstractTable
{
	private Collection<Object[]> data;
	private DataTable superDataTable;
	private final Table table;

	/**
	 * Constructor.
	 * 
	 * @param schema
	 *            The schema this table is in
	 * @param tableDef
	 *            The table definition to use.
	 */
	public DataTable( final Table table )
	{
		super(table.getSchema(), table);
		this.table = table;
		final Key key = table.getTableDef().getSortKey();
		if (key == null)
		{
			data = new ArrayList<Object[]>();
		}
		else
		{
			if (key.isUnique())
			{
				data = new TreeSet<Object[]>(key);
			}
			else
			{
				// supress warning is for this conversion as TreeBag is not
				// generic.
				data = new SortedBag<Object[]>(key);
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
		verify(args);
		data.add(args);
	}

	@Override
	public void delete()
	{
		table.delete();
	}

	@Override
	public Column getColumn( final int idx )
	{
		return table.getColumn(idx);
	}

	@Override
	public Column getColumn( final String name )
	{
		return table.getColumn(name);
	}

	@Override
	public int getColumnIndex( final String name )
	{
		return table.getColumnIndex(name);
	}

	@Override
	public String getName()
	{
		return table.getName();
	}

	@Override
	public Resource getResource()
	{
		return table.getResource();
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

	@Override
	public DataTable getSuperTable()
	{
		if (superDataTable == null)
		{
			if (getSuperTableDef() == null)
			{
				return null;
			}
			superDataTable = new DataTable(table.getSuperTable());
		}
		return superDataTable;
	}

	public boolean isEmpty()
	{
		return data.isEmpty();
	}
}
