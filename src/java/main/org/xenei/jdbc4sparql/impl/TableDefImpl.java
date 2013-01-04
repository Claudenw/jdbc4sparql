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

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jdbc4sparql.iface.SortKey;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.TypeConverter;

public class TableDefImpl extends NamespaceImpl implements TableDef
{
	// private String name;
	private final List<ColumnDef> columns;
	private SortKey sortKey;

	public TableDefImpl( final String namespace, final String name )
	{
		super(namespace, name);
		this.columns = new ArrayList<ColumnDef>();
		this.sortKey = null;
	}

	public void add( final ColumnDef column )
	{
		columns.add(column);
	}

	public void addKey( final ColumnDef columnDef )
	{
		final int idx = columns.indexOf(columnDef);
		if (idx == -1)
		{
			throw new IllegalArgumentException(columnDef.getLabel()
					+ " is not in table");
		}
		if (sortKey == null)
		{
			sortKey = new SortKey();
		}
		sortKey.addSegment(new KeySegment(idx, columnDef));
	}

	public void addKey( final String columnName )
	{
		addKey(getColumnDef(columnName));
	}

	@Override
	public int getColumnCount()
	{
		return columns.size();
	}

	@Override
	public ColumnDef getColumnDef( final int idx )
	{
		return columns.get(idx);
	}

	@Override
	public ColumnDef getColumnDef( final String name )
	{
		for (final ColumnDef retval : columns)
		{
			if (retval.getLabel().equals(name))
			{
				return retval;
			}
		}
		return null;
	}

	@Override
	public List<ColumnDef> getColumnDefs()
	{
		return columns;
	}

	@Override
	public int getColumnIndex( final ColumnDef column )
	{
		return columns.indexOf(column);
	}

	@Override
	public int getColumnIndex( final String columnName )
	{
		for (int i = 0; i < columns.size(); i++)
		{
			if (columns.get(i).getLabel().equals(columnName))
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public SortKey getSortKey()
	{
		return sortKey;
	}

	public void setUnique()
	{
		if (sortKey == null)
		{
			sortKey = new SortKey();
		}
		sortKey.setUnique();
	}

	@Override
	public String toString()
	{
		return String.format("TableDef[ %s : %s columns ]", getLocalName(),
				columns.size());
	}

	@Override
	public void verify( final Object[] row )
	{
		if (row.length != columns.size())
		{
			throw new IllegalArgumentException(String.format(
					"Expected %s columns but got %s", columns.size(),
					row.length));
		}
		for (int i = 0; i < row.length; i++)
		{
			final ColumnDef c = columns.get(i);

			if (row[i] == null)
			{
				if (c.getNullable() == DatabaseMetaData.columnNoNulls)
				{
					throw new IllegalArgumentException(String.format(
							"Column %s may not be null", c.getLabel()));
				}
			}
			else
			{
				final Class<?> clazz = TypeConverter.getJavaType(c.getType());
				if (!clazz.isAssignableFrom(row[i].getClass()))
				{
					throw new IllegalArgumentException(String.format(
							"Column %s can not recieve values of class %s",
							c.getLabel(), row[i].getClass()));
				}
			}
		}

	}

}