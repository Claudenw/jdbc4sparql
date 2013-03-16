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
package org.xenei.jdbc4sparql.meta;

import java.sql.Types;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.ColumnDefImpl;
import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * Meta column can be created without a table being specified first.
 */
public class MetaColumn extends ColumnDefImpl implements Column
{
	/**
	 * Get an integer instance of a meta column.
	 * @param localName The local name
	 * @return a MetaColumn
	 */
	public static MetaColumn getIntInstance( final String localName )
	{
		return new MetaColumn(localName, Types.INTEGER, 0, 0, 0, true);
	}

	/**
	 * Get a string (varchar) instance of a meta column
	 * @param localName The local name
	 * @return a MetaColumn
	 */
	public static MetaColumn getStringInstance( final String localName )
	{
		return new MetaColumn(localName, Types.VARCHAR, 0, 0, 0, false);
	}

	// the table the metacolumn is in.
	private Table table;

	/**
	 * Constructor.
	 * @param localName The local name
	 * @param type The column Type.
	 */
	public MetaColumn( final String localName, final int type )
	{
		this(localName, type, 0, 0, 0, true);
	}

	/**
	 * Constructor
	 * @param localName The local name
	 * @param type The column type.
	 * @param displaySize The display size.
	 * @param precision the percision.
	 * @param scale the scale.
	 * @param signed the signed flag.
	 */
	public MetaColumn( final String localName, final int type,
			final int displaySize, final int precision, final int scale,
			final boolean signed )
	{
		super(MetaNamespace.NS, localName, type, displaySize, precision, scale,
				signed);
	}

	@Override
	public Catalog getCatalog()
	{
		return table.getSchema().getCatalog();
	}

	@Override
	public String getSQLName()
	{
		return NameUtils.getDBName(this);
	}

	@Override
	public Schema getSchema()
	{
		return table.getSchema();
	}

	@Override
	public Table getTable()
	{
		return table;
	}

	/**
	 * Set the table the metacolumn is in.
	 * @param table
	 */
	void setTable( final Table table )
	{
		this.table = table;
	}

	@Override
	public String getSPARQLName()
	{
		return NameUtils.getSPARQLName(this);
	}

}