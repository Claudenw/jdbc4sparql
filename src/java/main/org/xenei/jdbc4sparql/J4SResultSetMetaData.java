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
package org.xenei.jdbc4sparql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TypeConverter;

public class J4SResultSetMetaData implements ResultSetMetaData
{
	private final Table table;

	public J4SResultSetMetaData( final Table table )
	{
		this.table = table;
	}

	@Override
	public String getCatalogName( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getName();
	}

	private Column getColumn( final int columnOrdinal )
	{
		return table.getColumn(columnOrdinal - 1);
	}

	@Override
	public String getColumnClassName( final int columnOrdinal )
			throws SQLException
	{
		return getColumn(columnOrdinal).getColumnClassName();
	}

	@Override
	public int getColumnCount() throws SQLException
	{
		return table.getColumnCount();
	}

	@Override
	public int getColumnDisplaySize( final int columnOrdinal )
			throws SQLException
	{
		return getColumn(columnOrdinal).getDisplaySize();
	}

	@Override
	public String getColumnLabel( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getLabel();
	}

	@Override
	public String getColumnName( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getName();
	}

	@Override
	public int getColumnType( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getType();
	}

	@Override
	public String getColumnTypeName( final int columnOrdinal )
			throws SQLException
	{
		final Class<?> typeClass = TypeConverter
				.getJavaType(getColumnType(columnOrdinal));
		return typeClass == null ? "UNKNOWN" : typeClass.getName();
	}

	@Override
	public int getPrecision( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getPrecision();
	}

	@Override
	public int getScale( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getScale();
	}

	@Override
	public String getSchemaName( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getSchema().getName();
	}

	@Override
	public String getTableName( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getTable().getName();
	}

	@Override
	public boolean isAutoIncrement( final int columnOrdinal )
			throws SQLException
	{
		return getColumn(columnOrdinal).isAutoIncrement();
	}

	@Override
	public boolean isCaseSensitive( final int columnOrdinal )
			throws SQLException
	{
		return getColumn(columnOrdinal).isCaseSensitive();
	}

	@Override
	public boolean isCurrency( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isCurrency();
	}

	@Override
	public boolean isDefinitelyWritable( final int columnOrdinal )
			throws SQLException
	{
		return getColumn(columnOrdinal).isDefinitelyWritable();
	}

	@Override
	public int isNullable( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getNullable();
	}

	@Override
	public boolean isReadOnly( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isReadOnly();
	}

	@Override
	public boolean isSearchable( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isSearchable();
	}

	@Override
	public boolean isSigned( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isSigned();
	}

	@Override
	public boolean isWrapperFor( final Class<?> iface ) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isWritable( final int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isWritable();
	}

	@Override
	public <T> T unwrap( final Class<T> iface ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

}
