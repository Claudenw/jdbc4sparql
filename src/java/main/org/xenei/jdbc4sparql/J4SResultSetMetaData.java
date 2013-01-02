package org.xenei.jdbc4sparql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TypeConverter;

public class J4SResultSetMetaData implements ResultSetMetaData
{
	private Table table;
	public J4SResultSetMetaData(Table table)
	{
		this.table=table;
	}

	@Override
	public boolean isWrapperFor( Class<?> iface ) throws SQLException
	{
		return false;
	}

	@Override
	public <T> T unwrap( Class<T> iface ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	private Column getColumn( int columnOrdinal )
	{
		return table.getColumn( columnOrdinal-1 );
	}
	@Override
	public String getCatalogName( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getLocalName();
	}

	@Override
	public String getColumnClassName( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getColumnClassName();
	}

	@Override
	public int getColumnCount() throws SQLException
	{
		return table.getColumnCount();
	}

	@Override
	public int getColumnDisplaySize( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getDisplaySize();
	}

	@Override
	public String getColumnLabel( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getLabel();
	}

	@Override
	public String getColumnName( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getLocalName();
	}

	@Override
	public int getColumnType( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getType();
	}

	@Override
	public String getColumnTypeName( int columnOrdinal ) throws SQLException
	{
		Class<?> typeClass = TypeConverter.getJavaType(getColumnType(columnOrdinal));
		return typeClass==null?"UNKNOWN":typeClass.getName();
	}

	@Override
	public int getPrecision( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getPrecision();
	}

	@Override
	public int getScale( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getScale();
	}

	@Override
	public String getSchemaName( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getSchema().getLocalName();
	}

	@Override
	public String getTableName( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getTable().getLocalName();
	}

	@Override
	public boolean isAutoIncrement( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isAutoIncrement();
	}

	@Override
	public boolean isCaseSensitive( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isCaseSensitive();
	}

	@Override
	public boolean isCurrency( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isCurrency();
	}

	@Override
	public boolean isDefinitelyWritable( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isDefinitelyWritable();
	}

	@Override
	public int isNullable( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).getNullable();
	}

	@Override
	public boolean isReadOnly( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isReadOnly();
	}

	@Override
	public boolean isSearchable( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isSearchable();
	}

	@Override
	public boolean isSigned( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isSigned();
	}

	@Override
	public boolean isWritable( int columnOrdinal ) throws SQLException
	{
		return getColumn(columnOrdinal).isWritable();
	}

}
