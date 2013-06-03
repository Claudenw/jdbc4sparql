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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.iface.TypeConverter;

/**
 * An abstract table implementation
 */
public abstract class AbstractTable implements Table
{
	// the table definition
	private final TableDef tableDef;
	// the schema this table is in.
	private final Schema schema;

	/**
	 * Constructor
	 * 
	 * @param schema
	 *            The schema that table is in
	 * @param tableDef
	 *            The definition of the table.
	 */
	public AbstractTable( final Schema schema, final TableDef tableDef )
	{
		this.schema = schema;
		this.tableDef = tableDef;
	}

	@Override
	public NameFilter<Column> findColumns( final String columnNamePattern )
	{
		return new NameFilter<Column>(columnNamePattern, getColumns());
	}

	@Override
	public Catalog getCatalog()
	{
		return schema.getCatalog();
	}

	@Override
	public int getColumnCount()
	{
		return tableDef.getColumnCount();
	}


	public ColumnDef getColumnDef( final int idx )
	{
		return tableDef.getColumnDef(idx);
	}

	
	public List<ColumnDef> getColumnDefs()
	{
		return tableDef.getColumnDefs();
	}


	public int getColumnIndex( final ColumnDef column )
	{
		return tableDef.getColumnIndex(column);
	}

	

	@Override
	public Iterator<? extends Column> getColumns()
	{
		return new Table.ColumnIterator(this, getColumnDefs());
	}



	public Key getPrimaryKey()
	{
		return tableDef.getPrimaryKey();
	}

	abstract public ResultSet getResultSet() throws SQLException;

	@Override
	public Schema getSchema()
	{
		return schema;
	}


	public Key getSortKey()
	{
		return tableDef.getSortKey();
	}

	@Override
	public String getSPARQLName()
	{
		return NameUtils.getSPARQLName(this);
	}

	@Override
	public String getSQLName()
	{
		return NameUtils.getDBName(this);
	}


	public TableDef getSuperTableDef()
	{
		return tableDef.getSuperTableDef();
	}

	/**
	 * @return The table definition for this table.
	 */
	public TableDef getTableDef()
	{
		return tableDef;
	}

	@Override
	public String getType()
	{
		return "TABLE";
	}

	@Override
	public String toString()
	{
		return String.format("Table[ %s.%s ]", getCatalog().getName(),
				getSQLName());
	}

	public void verify( final Object[] row )
	{
		List<ColumnDef> columns = tableDef.getColumnDefs();
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
							"Column %s may not be null", getColumn(i).getName()));
				}
			}
			else
			{
				final Class<?> clazz = TypeConverter.getJavaType(c.getType());
				if (!clazz.isAssignableFrom(row[i].getClass()))
				{
					throw new IllegalArgumentException(String.format(
							"Column %s can not receive values of class %s",
							getColumn(i).getName(), row[i].getClass()));
				}
			}
		}

	}
}
