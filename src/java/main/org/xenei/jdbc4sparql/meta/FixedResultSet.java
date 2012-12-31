package org.xenei.jdbc4sparql.meta;

import java.io.ByteArrayInputStream;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.AbstractResultSet;

public class FixedResultSet extends AbstractResultSet
{
	private List<Object[]> table;

	private int position;
	
	public FixedResultSet( Collection<Object[]> rows, Table table )
	{
		super( table );
		this.table = new ArrayList<Object[]>(rows);
		this.position = -1;
	}

	@Override
	public boolean isWrapperFor( Class<?> iface ) throws SQLException
	{
		return false;
	}

	@Override
	public <T> T unwrap( Class<T> iface ) throws SQLException
	{
		return null;
	}

	private boolean isValidPosition()
	{
		return position < table.size() && position>-1;
	}
	
	private void fixupPosition()
	{
		if (position < -1)
		{
			position = -1;
		}
		if (position > table.size())
		{
			position = table.size();
		}
	}
	@Override
	public boolean absolute( int pos ) throws SQLException
	{
		if ( pos < 0 )
		{
			this.position = table.size()-pos-1;;
		}
		else
		{
			this.position = pos-1;
		}
		fixupPosition();
		return isValidPosition();
	}

	@Override
	public void afterLast() throws SQLException
	{
		position = table.size();
	}

	@Override
	public void beforeFirst() throws SQLException
	{
		position = -1;
	}

	@Override
	public void cancelRowUpdates() throws SQLException
	{
		// do nothing -- not updatable
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		// do nothing.
	}

	@Override
	public void close() throws SQLException
	{
		// do nothing
	}

	@Override
	public void deleteRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean first() throws SQLException
	{
		position = 0;
		if (table.size() == 0)
		{
			throw new SQLException( "No rows in result set" );
		}
		return true;
	}
	
	protected void checkPosition() throws SQLException
	{
		if (! isValidPosition() )
		{
			throw new SQLException( "Cursor not positioned on a result");
		}
	}

	public Object getObject( int col ) throws SQLException
	{
		checkPosition();
		checkColumn( col );
		return table.get(position)[col-1];
	}
	
	private Object getColumnData( String col ) throws SQLException
	{
		checkPosition();
		return table.get(position)[findColumn(col)];
	}
	
	@Override
	public Array getArray( int col ) throws SQLException
	{
		checkType( col, Types.ARRAY );
		return (Array) getObject( col );
	}

	@Override
	public Array getArray( String col ) throws SQLException
	{
		return getArray( findColumn( col ) );
	}

	@Override
	public InputStream getAsciiStream( int col ) throws SQLException
	{
		return new ByteArrayInputStream( (byte[]) getObject( col ) );
	}

	@Override
	public InputStream getAsciiStream( String col ) throws SQLException
	{
		return new ByteArrayInputStream( (byte[]) getColumnData( col ) );
	}

	@Override
	public BigDecimal getBigDecimal( int col ) throws SQLException
	{
		checkType( col, Types.DECIMAL );
		return (BigDecimal) getObject( col );
	}

	@Override
	public BigDecimal getBigDecimal( String col ) throws SQLException
	{
		return getBigDecimal( findColumn( col ));
	}

	@Override
	public BigDecimal getBigDecimal( int col, int scale ) throws SQLException
	{
		return getBigDecimal( col );
	}

	@Override
	public BigDecimal getBigDecimal( String col, int scale ) throws SQLException
	{
		return getBigDecimal( col );
	}

	@Override
	public InputStream getBinaryStream( int col ) throws SQLException
	{
		return new ByteArrayInputStream( (byte[]) getObject( col ) );
	}

	@Override
	public InputStream getBinaryStream( String col ) throws SQLException
	{
		return getBinaryStream( findColumn( col ));
	}

	@Override
	public Blob getBlob( int col ) throws SQLException
	{
		checkType( col, Types.BLOB );
		return (Blob) getObject( col );
	}


	@Override
	public Blob getBlob( String col ) throws SQLException
	{
		return getBlob( findColumn( col ));
	}

	@Override
	public boolean getBoolean( int col ) throws SQLException
	{
		checkType( col, Types.BOOLEAN);
		return (Boolean) getObject( col );
	}

	@Override
	public boolean getBoolean( String col ) throws SQLException
	{
		return getBoolean( findColumn( col ));
	}

	@Override
	public byte getByte( int col ) throws SQLException
	{
		checkType( col, Types.INTEGER);
		return ((Integer)getObject(col)).byteValue();
	}

	@Override
	public byte getByte( String col ) throws SQLException
	{
		return getByte( findColumn( col ));
	}

	@Override
	public byte[] getBytes( int col ) throws SQLException
	{
		return ((byte[])getObject(col));
	}

	@Override
	public byte[] getBytes( String col ) throws SQLException
	{
		return getBytes( findColumn( col ));
	}

	@Override
	public Reader getCharacterStream( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob( String col ) throws SQLException
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate( int col, Calendar arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate( String col, Calendar arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getDouble( int col ) throws SQLException
	{
		checkType( col, Types.DOUBLE );
		return (Double) getObject( col );
	}

	@Override
	public double getDouble( String col ) throws SQLException
	{
		return getDouble( findColumn( col ));
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat( int col ) throws SQLException
	{
		checkType( col, Types.FLOAT );
		return (Float) getObject( col );
	}

	@Override
	public float getFloat( String col ) throws SQLException
	{
		return getFloat( findColumn( col ));
	}

	@Override
	public int getHoldability() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt( int col ) throws SQLException
	{
		checkType( col, Types.INTEGER );
		return (Integer) getObject( col );

	}

	@Override
	public int getInt( String col ) throws SQLException
	{
		return getInt( findColumn( col ));
	}

	@Override
	public long getLong( int col ) throws SQLException
	{
		checkType( col, Types.INTEGER );
		return (Long) getObject( col );

	}

	@Override
	public long getLong( String col ) throws SQLException
	{
		return getLong( findColumn( col ));
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	

	@Override
	public Object getObject( String col ) throws SQLException
	{
		return getObject( findColumn( col ));
	}

	@Override
	public Object getObject( int col, Map<String, Class<?>> arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject( String col, Map<String, Class<?>> arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef( String col ) throws SQLException
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
	public RowId getRowId( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public short getShort( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort( String col ) throws SQLException
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
	public String getString( int col ) throws SQLException
	{

		return getObject( col ).toString();
	}

	@Override
	public String getString( String col ) throws SQLException
	{
		return getString( findColumn( col ));
	}

	@Override
	public Time getTime( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime( int col, Calendar arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime( String col, Calendar arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( int col, Calendar arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( String col, Calendar arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getType() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public URL getURL( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertRow() throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		return position >= table.size();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		return position < 0;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException
	{
		return position == 0;
	}

	@Override
	public boolean isLast() throws SQLException
	{
		return position == table.size()-1;
	}

	@Override
	public boolean last() throws SQLException
	{
		position = table.size()-1;
		if (table.size() == 0)
		{
			throw new SQLException( "No rows in result set" );
		}
		return true;
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void moveToInsertRow() throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean next() throws SQLException
	{
		position++;
		fixupPosition();
		return !isAfterLast();
	}

	@Override
	public boolean previous() throws SQLException
	{
		position--;
		fixupPosition();
		return !isBeforeFirst();
	}

	@Override
	public void refreshRow() throws SQLException
	{
		// does nothing
	}

	@Override
	public boolean relative( int pos ) throws SQLException
	{
		position += pos;
		fixupPosition();
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setFetchDirection( int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setFetchSize( int arg0 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateArray( int col, Array arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateArray( String col, Array arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateAsciiStream( int col, InputStream arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateAsciiStream( String col, InputStream arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateAsciiStream( int col, InputStream arg1, int arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateAsciiStream( String col, InputStream arg1, int arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateAsciiStream( int col, InputStream arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateAsciiStream( String col, InputStream arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBigDecimal( int col, BigDecimal arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBigDecimal( String col, BigDecimal arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBinaryStream( int col, InputStream arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBinaryStream( String col, InputStream arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBinaryStream( int col, InputStream arg1, int arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBinaryStream( String col, InputStream arg1, int arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBinaryStream( int col, InputStream arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBinaryStream( String col, InputStream arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBlob( int col, Blob arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBlob( String col, Blob arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBlob( int col, InputStream arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBlob( String col, InputStream arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBlob( int col, InputStream arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBlob( String col, InputStream arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBoolean( int col, boolean arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBoolean( String col, boolean arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateByte( int col, byte arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateByte( String col, byte arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBytes( int col, byte[] arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateBytes( String col, byte[] arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateCharacterStream( int col, Reader arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateCharacterStream( String col, Reader arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateCharacterStream( int col, Reader arg1, int arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateCharacterStream( String col, Reader arg1, int arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateCharacterStream( int col, Reader arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateCharacterStream( String col, Reader arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateClob( int col, Clob arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateClob( String col, Clob arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateClob( int col, Reader arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateClob( String col, Reader arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateClob( int col, Reader arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateClob( String col, Reader arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateDate( int col, Date arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateDate( String col, Date arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateDouble( int col, double arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateDouble( String col, double arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateFloat( int col, float arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateFloat( String col, float arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateInt( int col, int arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateInt( String col, int arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateLong( int col, long arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateLong( String col, long arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNCharacterStream( int col, Reader arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNCharacterStream( String col, Reader arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNCharacterStream( int col, Reader arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNCharacterStream( String col, Reader arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNClob( int col, NClob arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNClob( String col, NClob arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNClob( int col, Reader arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNClob( String col, Reader arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNClob( int col, Reader arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNClob( String col, Reader arg1, long arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNString( int col, String arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNString( String col, String arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNull( int col ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateNull( String col ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateObject( int col, Object arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateObject( String col, Object arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateObject( int col, Object arg1, int arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateObject( String col, Object arg1, int arg2 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateRef( int col, Ref arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateRef( String col, Ref arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateRow() throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateRowId( int col, RowId arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateRowId( String col, RowId arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateSQLXML( int col, SQLXML arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateSQLXML( String col, SQLXML arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateShort( int col, short arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateShort( String col, short arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateString( int col, String arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateString( String col, String arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateTime( int col, Time arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateTime( String col, Time arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateTimestamp( int col, Timestamp arg1 ) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateTimestamp( String col, Timestamp arg1 )
			throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean wasNull() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T getObject( int columnIndex, Class<T> type )
			throws SQLException
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

}
