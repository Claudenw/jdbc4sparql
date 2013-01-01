package org.xenei.jdbc4sparql.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
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

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.apache.commons.io.IOUtils;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public abstract class AbstractResultSet implements ResultSet
{
	private final Table table;
	private final Map<String, Integer> columnNameIdx;
	private int fetchDirection;
	private int holdability;
	private Statement statement;

	public AbstractResultSet( final Table table )
	{
		this( table, null );
	}
	
	public AbstractResultSet( final Table table, Statement statement )
	{
		this.table = table;
		this.statement = statement;
		this.fetchDirection = ResultSet.FETCH_FORWARD;
		this.holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
		
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
	public Array getArray( int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray( String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, InputStream.class );
	}

	@Override
	public InputStream getAsciiStream( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), InputStream.class );
	}

	@Override
	public BigDecimal getBigDecimal( int columnOrdinal, int scale )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, BigDecimal.class );
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
		return extractData( table.getColumnIndex( columnLabel ), BigDecimal.class );
	}

	@Override
	public InputStream getBinaryStream( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, InputStream.class );
	}

	@Override
	public InputStream getBinaryStream( String columnLabel )
			throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), InputStream.class );
	}

	@Override
	public Blob getBlob( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Blob.class );
	}

	@Override
	public Blob getBlob( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Blob.class );
	}

	@Override
	public boolean getBoolean( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Boolean.class );
	}

	@Override
	public boolean getBoolean( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Boolean.class );
	}

	@Override
	public byte getByte( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Byte.class );
	}

	@Override
	public byte getByte( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Byte.class );
	}

	@Override
	public byte[] getBytes( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, byte[].class );
	}

	@Override
	public byte[] getBytes( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), byte[].class );
	}

	@Override
	public Reader getCharacterStream( int columnOrdinal ) throws SQLException
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
	public Clob getClob( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Clob.class );
	}

	@Override
	public Clob getClob( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Clob.class );
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
	public Date getDate( int columnOrdinal, Calendar cal ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate( int columnOrdinal ) throws SQLException
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
	public double getDouble( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Double.class );
	}

	@Override
	public double getDouble( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Double.class );
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
	public float getFloat( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Float.class );
	}

	@Override
	public float getFloat( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Float.class );
	}

	@Override
	public int getHoldability() throws SQLException
	{
		return holdability;
	}

	@Override
	public int getInt( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Integer.class );
	}

	@Override
	public int getInt( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Integer.class );
	}

	@Override
	public long getLong( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Long.class );
	}

	@Override
	public long getLong( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Long.class );
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream( int columnOrdinal ) throws SQLException
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
	public NClob getNClob( int columnOrdinal ) throws SQLException
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
	public String getNString( int columnOrdinal ) throws SQLException
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
	public <T> T getObject( int columnOrdinal, Class<T> type )
			throws SQLException
	{
		return extractData( columnOrdinal-1, type );
	}

	@Override
	public Object getObject( int columnOrdinal, Map<String, Class<?>> map )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Return the data object for the column from the dataset.
	 */
	@Override
	abstract public Object getObject( int columnOrdinal ) throws SQLException;

	@Override
	public <T> T getObject( String columnLabel, Class<T> type )
			throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), type );
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
		return getObject( table.getColumnIndex( columnLabel )+1 );
	}

	@Override
	public Ref getRef( int columnOrdinal ) throws SQLException
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
	public RowId getRowId( int columnOrdinal ) throws SQLException
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
	public SQLXML getSQLXML( int columnOrdinal ) throws SQLException
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
	public short getShort( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Short.class );
	}

	@Override
	public short getShort( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), Short.class );
	}

	@Override
	public Statement getStatement() throws SQLException
	{
		return statement;
	}

	@Override
	public String getString( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, String.class );
	}

	@Override
	public String getString( String columnLabel ) throws SQLException
	{
		return extractData( table.getColumnIndex( columnLabel ), String.class );
	}

	@Override
	public Time getTime( int columnOrdinal, Calendar cal ) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime( int columnOrdinal ) throws SQLException
	{
		return extractData( columnOrdinal-1, Time.class );
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
		return extractData( table.getColumnIndex( columnLabel ), Time.class );
	}

	@Override
	public Timestamp getTimestamp( int columnOrdinal, Calendar cal )
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp( int columnOrdinal ) throws SQLException
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
	public URL getURL( int columnOrdinal ) throws SQLException
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
	public InputStream getUnicodeStream( int columnOrdinal ) throws SQLException
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
	

	private <T> T extractData( int columnIdx, Class<T> resultingClass) throws SQLException
	{
		Object columnObject = getObject( columnIdx+1 );
		T retval = null;
		
		// try the simple case
		if (resultingClass.isAssignableFrom( columnObject.getClass() ))
		{
			retval = resultingClass.cast( columnObject );
		}
		
		// see if we can do a simple numeric assignment
		if (retval == null && (columnObject instanceof Number))
		{
			retval = fromNumber( columnObject, resultingClass );
		}
		
		// see if we can convert from a string
		if (retval == null && (columnObject instanceof String))
		{
			retval = fromString( columnObject, resultingClass );
		}
		
		if (retval == null && (columnObject instanceof Boolean))
		{
			Boolean b = (Boolean) columnObject;
			retval = fromString( b?"1":"0", resultingClass );
		}
		
		if (retval == null && (columnObject instanceof byte[]))
		{
			retval = fromByteArray( columnObject, resultingClass );	
		}
		
		if (retval == null && (columnObject instanceof Blob))
		{
			retval = fromByteArray( columnObject, resultingClass );	
		}
		if (retval == null && (columnObject instanceof Clob ))
		{
			retval = fromByteArray( columnObject, resultingClass );	
		}
		if (retval == null && (columnObject instanceof InputStream ))
		{
			retval = fromByteArray( columnObject, resultingClass );	
		}
		// if null result then throw an exception
		if (retval == null)
		{
			throw new SQLException(String.format(" Can not cast %s to %s", columnObject.getClass(), resultingClass));
		}
		return retval;
	}
	
	/**
	 * Handles things that are or are like byte arrays.
	 * byte[], Clob, Blob, InputStream
	 * @param columnObject
	 * @param resultingClass
	 * @return
	 * @throws SQLException
	 */
	private <T> T fromByteArray(Object columnObject, Class<T> resultingClass) throws SQLException
	{
		String s = null;
		
		try
		{
			if (columnObject instanceof InputStream)
			{
				InputStream is = (InputStream)columnObject;
				if (resultingClass.isAssignableFrom( Clob.class ))
				{
					return resultingClass.cast(new SerialClob(IOUtils.toCharArray(is)));
				}
				if (resultingClass.isAssignableFrom( Blob.class ))
				{
					return resultingClass.cast( new SerialBlob(IOUtils.toByteArray(is)));
				}
				if (resultingClass.isAssignableFrom( byte[].class ))
				{
					return resultingClass.cast( IOUtils.toByteArray(is));
				}
				return fromString( new String( IOUtils.toByteArray(is)), resultingClass);
			}
			
			if (s == null && columnObject instanceof byte[])
			{
				if (resultingClass.isAssignableFrom( Clob.class ))
				{
					return resultingClass.cast(					
							new SerialClob(
									IOUtils.toCharArray(new ByteArrayInputStream((byte[])columnObject))));
				}
				if (resultingClass.isAssignableFrom( Blob.class ))
				{
					return resultingClass.cast( new SerialBlob((byte[])columnObject));
				}
				if (resultingClass.isAssignableFrom( InputStream.class ))
				{
					return resultingClass.cast( new ByteArrayInputStream((byte[])columnObject));
				}
				s = new String( (byte[]) columnObject);
			}
			if (s == null && columnObject instanceof Clob)
			{
				Clob c = (Clob)columnObject;
				if (resultingClass.isAssignableFrom( byte[].class ))
				{
					return resultingClass.cast(
							IOUtils.toByteArray(c.getAsciiStream()));
				}
				if (resultingClass.isAssignableFrom( Blob.class ))
				{
					return resultingClass.cast( new SerialBlob(IOUtils.toByteArray(
							c.getAsciiStream())));
				}
				if (resultingClass.isAssignableFrom( InputStream.class ))
				{
					return resultingClass.cast( c.getAsciiStream());
				}
				 s = String.valueOf( IOUtils.toCharArray(c.getCharacterStream()));
			}
			if (s == null && columnObject instanceof Blob)
			{
				Blob b = (Blob) columnObject;
				if (resultingClass.isAssignableFrom( byte[].class ))
				{
					return resultingClass.cast(
							IOUtils.toByteArray(b.getBinaryStream()));
				}
				if (resultingClass.isAssignableFrom( Clob.class ))
				{
					return resultingClass.cast( new SerialClob(IOUtils.toCharArray(
							b.getBinaryStream())));
				}
				if (resultingClass.isAssignableFrom( InputStream.class ))
				{
					return resultingClass.cast( b.getBinaryStream());
				}
				s = new String( IOUtils.toByteArray(((Blob)columnObject).getBinaryStream()));
			}
			
			if (s != null)
			{
				return fromString( s, resultingClass );
			}
			return null;
		}
		catch (IOException e)
		{
			throw new SQLException( e );
		}
	}
	
	private <T> T  fromString(Object columnObject, Class<T> resultingClass) throws SQLException
	{
		String val = String.class.cast( columnObject );
		if (resultingClass == BigDecimal.class)
		{
			return resultingClass.cast(new BigDecimal( val ));
		}
		if (resultingClass == BigInteger.class)
		{
			return resultingClass.cast(new BigInteger( val ));
		}
		if (resultingClass == Byte.class)
		{
			return 	resultingClass.cast(new Byte( val ));
		}
		if (resultingClass == Double.class)
		{
			return resultingClass.cast(new Double( val ));
		}
		if (resultingClass == Float.class)
		{
			return resultingClass.cast(new Float( val ));
		}
		if (resultingClass == Integer.class)
		{
			return resultingClass.cast(new Integer( val ));
		}
		if (resultingClass == Long.class)
		{
			return resultingClass.cast(new Long( val ));
		}
		if (resultingClass == Short.class)
		{
			return resultingClass.cast(new Short( val ));
		}
		if (resultingClass == Boolean.class)
		{
			if ("0".equals( val ))
			{
				return resultingClass.cast(Boolean.FALSE);
			}
			if ("1".equals( val ))
			{
				return resultingClass.cast(Boolean.TRUE);
			}
		}
		if (resultingClass == byte[].class)
		{
			return resultingClass.cast( val.getBytes() );
		}
		if (resultingClass == Blob.class)
		{
			return resultingClass.cast( new SerialBlob( val.getBytes() ));
		}
		if (resultingClass == Clob.class )
		{
			return resultingClass.cast( new SerialClob( val.toCharArray() ));
		}
		return null;
	}
	
	private <T> T fromNumber(Object columnObject, Class<T> resultingClass) throws SQLException
	{
		Number n = Number.class.cast( columnObject );
		if (resultingClass == BigDecimal.class)
		{
			return resultingClass.cast(new BigDecimal( n.toString() ));
		}
		if (resultingClass == BigInteger.class)
		{
			return resultingClass.cast(new BigInteger( n.toString() ));
		}
		if (resultingClass == Byte.class)
		{
			return 	resultingClass.cast(new Byte(n.byteValue()));
		}
		if (resultingClass == Double.class)
		{
			return resultingClass.cast(new Double( n.doubleValue() ));
		}
		if (resultingClass == Float.class)
		{
			return resultingClass.cast(new Float( n.floatValue() ));
		}
		if (resultingClass == Integer.class)
		{
			return resultingClass.cast(new Integer( n.intValue()));
		}
		if (resultingClass == Long.class)
		{
			return resultingClass.cast(new Long( n.longValue() ));
		}
		if (resultingClass == Short.class)
		{
			return resultingClass.cast(new Short( n.shortValue() ));
		}
		if (resultingClass == String.class)
		{
			return resultingClass.cast(n.toString());
		}
		if (resultingClass == Boolean.class)
		{
			if (n.byteValue() == 0)
			{
				return resultingClass.cast(Boolean.FALSE);
			}
			if (n.byteValue() == 1)
			{
				return resultingClass.cast(Boolean.TRUE);
			}
		}
		if (resultingClass == byte[].class)
		{
			return resultingClass.cast( n.toString().getBytes() );
		}
		if (resultingClass == Blob.class)
		{
			return resultingClass.cast( new SerialBlob( n.toString().getBytes() ));
		}
		if (resultingClass == Clob.class )
		{
			return resultingClass.cast( new SerialClob( n.toString().toCharArray() ));
		}
		return null;
	}
}
