package org.xenei.jdbc4sparql.impl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public abstract class AbstractResultSet implements ResultSet
{
	private final Table table;
	private final Map<String, Integer> columnNameIdx;
	private int fetchDirection;
	private int holdability;

	public AbstractResultSet( final Table table )
	{
		this.table = table;
		fetchDirection = ResultSet.FETCH_FORWARD;
		holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
		
		columnNameIdx = new HashMap<String, Integer>();
		for (int i = 0; i < table.getColumnDefs().size(); i++)
		{
			columnNameIdx.put(table.getColumn(i).getLabel(), i);
		}
	}

	protected void checkColumn( final int idx ) throws SQLException
	{
		if (!isValidColumn(idx))
		{
			throw new SQLException("Invalid column idex: " + idx);
		}
	}

	protected void checkType( final int idx, final int type )
			throws SQLException
	{
		final Column c = getColumn(idx);
		if (c.getType() != type)
		{
			throw new SQLException("Column type (" + c.getType() + ") is not "
					+ type);
		}
	}

	@Override
	public int findColumn( final String columnName ) throws SQLException
	{
		final Integer idx = columnNameIdx.get(columnName);
		if (idx == null)
		{
			throw new SQLException(columnName + " is not a column");
		}
		return idx;
	}

	/**
	 * Return the column at the ordinal location idx (e.g. 1 based).
	 * @param idx
	 * @return The column
	 * @throws SQLException
	 */
	protected Column getColumn( final int idx ) throws SQLException
	{
		checkColumn(idx);
		return table.getColumn(idx - 1);
	}

	protected Column getColumn( final String name ) throws SQLException
	{
		return table.getColumn(name);
	}

	protected boolean isValidColumn( final int idx )
	{
		return (idx > 0) && (idx <= table.getColumnCount());
	}

	@Override
	public boolean isWrapperFor( Class<?> iface ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap( Class<T> iface ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Array getArray( int columnIndex ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray( String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal( int columnIndex, int scale )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal( String columnLabel, int scale )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream( String columnLabel )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getBoolean( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getBoolean( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByte( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte getByte( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getBytes( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getConcurrency() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getCursorName() throws SQLException
	{
		return table.getFQName();
	}

	@Override
	public Date getDate( int columnIndex, Calendar cal ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate( String columnLabel, Calendar cal ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getDouble( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		return fetchDirection;
	}
	
	@Override
	public int getFetchSize() throws SQLException
	{
		return Integer.MAX_VALUE;
	}

	@Override
	public float getFloat( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getHoldability() throws SQLException
	{
		return holdability;
	}

	@Override
	public int getInt( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getObject( int columnIndex, Class<T> type )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject( int columnIndex, Map<String, Class<?>> map )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getObject( String columnLabel, Class<T> type )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject( String columnLabel, Map<String, Class<?>> map )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getRow() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RowId getRowId( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public short getShort( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Statement getStatement() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime( int columnIndex, Calendar cal ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime( String columnLabel, Calendar cal ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( int columnIndex, Calendar cal )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( String columnLabel, Calendar cal )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL( String columnLabel ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream( int columnIndex ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream( String columnLabel )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
