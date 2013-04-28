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

/**
 * An abstract table implementation
 */
public abstract class AbstractTable extends NamespaceImpl implements Table
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
		this(schema.getNamespace(), schema, tableDef);
	}

	/**
	 * Constructor.
	 * 
	 * @param namespace
	 *            The namespace of the table.
	 * @param schema
	 *            The schema for the table.
	 * @param tableDef
	 *            The table definition of the table.
	 */
	public AbstractTable( final String namespace, final Schema schema,
			final TableDef tableDef )
	{
		super(namespace, tableDef.getLocalName());
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
	public Column getColumn( final int idx )
	{
		return new ColumnImpl(this, getColumnDef(idx));
	}

	@Override
	public Column getColumn( final String name )
	{
		return new ColumnImpl(this, getColumnDef(name));
	}

	@Override
	public int getColumnCount()
	{
		return tableDef.getColumnCount();
	}

	@Override
	public ColumnDef getColumnDef( final int idx )
	{
		return tableDef.getColumnDef(idx);
	}

	@Override
	public ColumnDef getColumnDef( final String name )
	{
		return tableDef.getColumnDef(name);
	}

	@Override
	public List<ColumnDef> getColumnDefs()
	{
		return tableDef.getColumnDefs();
	}

	@Override
	public int getColumnIndex( final ColumnDef column )
	{
		return tableDef.getColumnIndex(column);
	}

	@Override
	public int getColumnIndex( final String columnName )
	{
		return tableDef.getColumnIndex(columnName);
	}

	@Override
	public Iterator<? extends Column> getColumns()
	{
		return new Table.ColumnIterator(this, getColumnDefs());
	}

	@Override
	public String getLocalName()
	{
		return tableDef.getLocalName();
	}

	@Override
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

	@Override
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

	@Override
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
		return String.format("Table[ %s.%s ]", getCatalog().getLocalName(),
				getSQLName());
	}

	@Override
	public void verify( final Object[] row )
	{
		tableDef.verify(row);
	}
}
