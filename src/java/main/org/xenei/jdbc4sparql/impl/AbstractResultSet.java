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
import java.sql.SQLWarning;
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
import org.xenei.jdbc4sparql.J4SResultSetMetaData;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;

public abstract class AbstractResultSet implements ResultSet
{
	private final Table table;
	private final Map<String, Integer> columnNameIdx;
	private int fetchDirection;
	private final int holdability;
	protected int concurrency;
	private final Statement statement;
	private Boolean lastReadWasNull;
	private static Map<Class<?>, Object> nullValueMap;

	static
	{
		AbstractResultSet.nullValueMap = new HashMap<Class<?>, Object>();
		AbstractResultSet.nullValueMap.put(Boolean.class, Boolean.FALSE);
		AbstractResultSet.nullValueMap.put(Byte.class, new Byte((byte) 0));
		AbstractResultSet.nullValueMap.put(Short.class, new Short((short) 0));
		AbstractResultSet.nullValueMap.put(Integer.class, new Integer(0));
		AbstractResultSet.nullValueMap.put(Long.class, new Long(0L));
		AbstractResultSet.nullValueMap.put(Float.class, new Float(0.0F));
		AbstractResultSet.nullValueMap.put(Double.class, new Double(0.0));
	}

	public AbstractResultSet( final Table table )
	{
		this(table, null);
	}

	public AbstractResultSet( final Table table, final Statement statement )
	{
		if (table == null)
		{
			throw new IllegalArgumentException("Table may not be null");
		}
		this.table = table;
		this.statement = statement;
		this.fetchDirection = ResultSet.FETCH_FORWARD;
		this.holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
		this.concurrency = ResultSet.CONCUR_READ_ONLY;
		columnNameIdx = new HashMap<String, Integer>();
		for (int i = 0; i < table.getColumnDefs().size(); i++)
		{
			columnNameIdx.put(table.getColumn(i).getLabel(), i);
		}
	}

	protected void checkColumn( final int columnOrdinal ) throws SQLException
	{
		if (!isValidColumn(columnOrdinal))
		{
			throw new SQLException("Invalid column ordinal: " + columnOrdinal);
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

	private <T> T extractData( final int columnIdx,
			final Class<T> resultingClass ) throws SQLException
	{
		final Object columnObject = getObject(columnIdx + 1);

		if (columnObject == null)
		{
			return (T) AbstractResultSet.nullValueMap.get(resultingClass);
		}
		T retval = null;

		// try the simple case
		if (resultingClass.isAssignableFrom(columnObject.getClass()))
		{
			retval = resultingClass.cast(columnObject);
		}

		// see if we can do a simple numeric assignment
		if ((retval == null) && (columnObject instanceof Number))
		{
			retval = fromNumber(columnObject, resultingClass);
		}

		// see if we can convert from a string
		if ((retval == null) && (columnObject instanceof String))
		{
			retval = fromString(columnObject, resultingClass);
		}

		if ((retval == null) && (columnObject instanceof Boolean))
		{
			final Boolean b = (Boolean) columnObject;
			retval = fromString(b ? "1" : "0", resultingClass);
		}

		if ((retval == null) && (columnObject instanceof byte[]))
		{
			retval = fromByteArray(columnObject, resultingClass);
		}

		if ((retval == null) && (columnObject instanceof Blob))
		{
			retval = fromByteArray(columnObject, resultingClass);
		}
		if ((retval == null) && (columnObject instanceof Clob))
		{
			retval = fromByteArray(columnObject, resultingClass);
		}
		if ((retval == null) && (columnObject instanceof InputStream))
		{
			retval = fromByteArray(columnObject, resultingClass);
		}
		// if null result then throw an exception
		if (retval == null)
		{
			throw new SQLException(String.format(" Can not cast %s to %s",
					columnObject.getClass(), resultingClass));
		}
		return retval;
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
	 * Handles things that are or are like byte arrays.
	 * byte[], Clob, Blob, InputStream
	 * 
	 * @param columnObject
	 * @param resultingClass
	 * @return
	 * @throws SQLException
	 */
	private <T> T fromByteArray( final Object columnObject,
			final Class<T> resultingClass ) throws SQLException
	{
		String s = null;

		try
		{
			if (columnObject instanceof InputStream)
			{
				final InputStream is = (InputStream) columnObject;
				if (resultingClass.isAssignableFrom(Clob.class))
				{
					return resultingClass.cast(new SerialClob(IOUtils
							.toCharArray(is)));
				}
				if (resultingClass.isAssignableFrom(Blob.class))
				{
					return resultingClass.cast(new SerialBlob(IOUtils
							.toByteArray(is)));
				}
				if (resultingClass.isAssignableFrom(byte[].class))
				{
					return resultingClass.cast(IOUtils.toByteArray(is));
				}
				return fromString(new String(IOUtils.toByteArray(is)),
						resultingClass);
			}

			if ((s == null) && (columnObject instanceof byte[]))
			{
				if (resultingClass.isAssignableFrom(Clob.class))
				{
					return resultingClass.cast(new SerialClob(IOUtils
							.toCharArray(new ByteArrayInputStream(
									(byte[]) columnObject))));
				}
				if (resultingClass.isAssignableFrom(Blob.class))
				{
					return resultingClass.cast(new SerialBlob(
							(byte[]) columnObject));
				}
				if (resultingClass.isAssignableFrom(InputStream.class))
				{
					return resultingClass.cast(new ByteArrayInputStream(
							(byte[]) columnObject));
				}
				s = new String((byte[]) columnObject);
			}
			if ((s == null) && (columnObject instanceof Clob))
			{
				final Clob c = (Clob) columnObject;
				if (resultingClass.isAssignableFrom(byte[].class))
				{
					return resultingClass.cast(IOUtils.toByteArray(c
							.getAsciiStream()));
				}
				if (resultingClass.isAssignableFrom(Blob.class))
				{
					return resultingClass.cast(new SerialBlob(IOUtils
							.toByteArray(c.getAsciiStream())));
				}
				if (resultingClass.isAssignableFrom(InputStream.class))
				{
					return resultingClass.cast(c.getAsciiStream());
				}
				s = String.valueOf(IOUtils.toCharArray(c.getCharacterStream()));
			}
			if ((s == null) && (columnObject instanceof Blob))
			{
				final Blob b = (Blob) columnObject;
				if (resultingClass.isAssignableFrom(byte[].class))
				{
					return resultingClass.cast(IOUtils.toByteArray(b
							.getBinaryStream()));
				}
				if (resultingClass.isAssignableFrom(Clob.class))
				{
					return resultingClass.cast(new SerialClob(IOUtils
							.toCharArray(b.getBinaryStream())));
				}
				if (resultingClass.isAssignableFrom(InputStream.class))
				{
					return resultingClass.cast(b.getBinaryStream());
				}
				s = new String(IOUtils.toByteArray(((Blob) columnObject)
						.getBinaryStream()));
			}

			if (s != null)
			{
				return fromString(s, resultingClass);
			}
			return null;
		}
		catch (final IOException e)
		{
			throw new SQLException(e);
		}
	}

	private <T> T fromNumber( final Object columnObject,
			final Class<T> resultingClass ) throws SQLException
	{
		final Number n = Number.class.cast(columnObject);
		if (resultingClass == BigDecimal.class)
		{
			return resultingClass.cast(new BigDecimal(n.toString()));
		}
		if (resultingClass == BigInteger.class)
		{
			return resultingClass.cast(new BigInteger(n.toString()));
		}
		if (resultingClass == Byte.class)
		{
			return resultingClass.cast(new Byte(n.byteValue()));
		}
		if (resultingClass == Double.class)
		{
			return resultingClass.cast(new Double(n.doubleValue()));
		}
		if (resultingClass == Float.class)
		{
			return resultingClass.cast(new Float(n.floatValue()));
		}
		if (resultingClass == Integer.class)
		{
			return resultingClass.cast(new Integer(n.intValue()));
		}
		if (resultingClass == Long.class)
		{
			return resultingClass.cast(new Long(n.longValue()));
		}
		if (resultingClass == Short.class)
		{
			return resultingClass.cast(new Short(n.shortValue()));
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
			return resultingClass.cast(n.toString().getBytes());
		}
		if (resultingClass == Blob.class)
		{
			return resultingClass.cast(new SerialBlob(n.toString().getBytes()));
		}
		if (resultingClass == Clob.class)
		{
			return resultingClass.cast(new SerialClob(n.toString()
					.toCharArray()));
		}
		return null;
	}

	private <T> T fromString( final Object columnObject,
			final Class<T> resultingClass ) throws SQLException
	{
		final String val = String.class.cast(columnObject);
		if (resultingClass == BigDecimal.class)
		{
			return resultingClass.cast(new BigDecimal(val));
		}
		if (resultingClass == BigInteger.class)
		{
			return resultingClass.cast(new BigInteger(val));
		}
		if (resultingClass == Byte.class)
		{
			return resultingClass.cast(new Byte(val));
		}
		if (resultingClass == Double.class)
		{
			return resultingClass.cast(new Double(val));
		}
		if (resultingClass == Float.class)
		{
			return resultingClass.cast(new Float(val));
		}
		if (resultingClass == Integer.class)
		{
			return resultingClass.cast(new Integer(val));
		}
		if (resultingClass == Long.class)
		{
			return resultingClass.cast(new Long(val));
		}
		if (resultingClass == Short.class)
		{
			return resultingClass.cast(new Short(val));
		}
		if (resultingClass == Boolean.class)
		{
			if ("0".equals(val))
			{
				return resultingClass.cast(Boolean.FALSE);
			}
			if ("1".equals(val))
			{
				return resultingClass.cast(Boolean.TRUE);
			}
		}
		if (resultingClass == byte[].class)
		{
			return resultingClass.cast(val.getBytes());
		}
		if (resultingClass == Blob.class)
		{
			return resultingClass.cast(new SerialBlob(val.getBytes()));
		}
		if (resultingClass == Clob.class)
		{
			return resultingClass.cast(new SerialClob(val.toCharArray()));
		}
		return null;
	}

	@Override
	public Array getArray( final int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream( final int columnOrdinal )
			throws SQLException
	{
		return extractData(columnOrdinal - 1, InputStream.class);
	}

	@Override
	public InputStream getAsciiStream( final String columnLabel )
			throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), InputStream.class);
	}

	@Override
	public BigDecimal getBigDecimal( final int columnOrdinal )
			throws SQLException
	{
		return extractData(columnOrdinal - 1, BigDecimal.class);
	}

	@Override
	public BigDecimal getBigDecimal( final int columnOrdinal, final int scale )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal( final String columnLabel )
			throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), BigDecimal.class);
	}

	@Override
	public BigDecimal getBigDecimal( final String columnLabel, final int scale )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream( final int columnOrdinal )
			throws SQLException
	{
		return extractData(columnOrdinal - 1, InputStream.class);
	}

	@Override
	public InputStream getBinaryStream( final String columnLabel )
			throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), InputStream.class);
	}

	@Override
	public Blob getBlob( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Blob.class);
	}

	@Override
	public Blob getBlob( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Blob.class);
	}

	@Override
	public boolean getBoolean( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Boolean.class);
	}

	@Override
	public boolean getBoolean( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Boolean.class);
	}

	@Override
	public byte getByte( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Byte.class);
	}

	@Override
	public byte getByte( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Byte.class);
	}

	@Override
	public byte[] getBytes( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, byte[].class);
	}

	@Override
	public byte[] getBytes( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), byte[].class);
	}

	@Override
	public Reader getCharacterStream( final int columnOrdinal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream( final String columnLabel )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Clob.class);
	}

	@Override
	public Clob getClob( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Clob.class);
	}

	/**
	 * Return the column at the ordinal location idx (e.g. 1 based).
	 * 
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

	@Override
	public int getConcurrency() throws SQLException
	{
		return concurrency;
	}

	@Override
	public String getCursorName() throws SQLException
	{
		return table.getFQName();
	}

	@Override
	public Date getDate( final int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate( final int columnOrdinal, final Calendar cal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate( final String columnLabel, final Calendar cal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public double getDouble( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Double.class);
	}

	@Override
	public double getDouble( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Double.class);
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
	public float getFloat( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Float.class);
	}

	@Override
	public float getFloat( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Float.class);
	}

	@Override
	public int getHoldability() throws SQLException
	{
		return holdability;
	}

	@Override
	public int getInt( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Integer.class);
	}

	@Override
	public int getInt( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Integer.class);
	}

	@Override
	public long getLong( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Long.class);
	}

	@Override
	public long getLong( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Long.class);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		return new J4SResultSetMetaData(table);
	}

	@Override
	public Reader getNCharacterStream( final int columnOrdinal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream( final String columnLabel )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob( final int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString( final int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	/**
	 * Return the data object for the column from the dataset.
	 */
	@Override
	public Object getObject( final int columnOrdinal ) throws SQLException
	{
		final Object retval = readObject(columnOrdinal);
		lastReadWasNull = retval == null;
		return retval;
	}

	@Override
	public <T> T getObject( final int columnOrdinal, final Class<T> type )
			throws SQLException
	{
		return extractData(columnOrdinal - 1, type);
	}

	@Override
	public Object getObject( final int columnOrdinal,
			final Map<String, Class<?>> map ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject( final String columnLabel ) throws SQLException
	{
		return getObject(table.getColumnIndex(columnLabel) + 1);
	}

	@Override
	public <T> T getObject( final String columnLabel, final Class<T> type )
			throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), type);
	}

	@Override
	public Object getObject( final String columnLabel,
			final Map<String, Class<?>> map ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef( final int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId( final int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public short getShort( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Short.class);
	}

	@Override
	public short getShort( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Short.class);
	}

	@Override
	public SQLXML getSQLXML( final int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement getStatement() throws SQLException
	{
		return statement;
	}

	@Override
	public String getString( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, String.class);
	}

	@Override
	public String getString( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), String.class);
	}

	protected Table getTable()
	{
		return table;
	}

	@Override
	public Time getTime( final int columnOrdinal ) throws SQLException
	{
		return extractData(columnOrdinal - 1, Time.class);
	}

	@Override
	public Time getTime( final int columnOrdinal, final Calendar cal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime( final String columnLabel ) throws SQLException
	{
		return extractData(table.getColumnIndex(columnLabel), Time.class);
	}

	@Override
	public Time getTime( final String columnLabel, final Calendar cal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp( final int columnOrdinal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp( final int columnOrdinal, final Calendar cal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp( final String columnLabel )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp( final String columnLabel, final Calendar cal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream( final int columnOrdinal )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream( final String columnLabel )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL( final int columnOrdinal ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLException("not an updatable result set");
	}

	protected boolean isValidColumn( final int columnOrdinal )
	{
		return (columnOrdinal > 0) && (columnOrdinal <= table.getColumnCount());
	}

	@Override
	public boolean isWrapperFor( final Class<?> iface ) throws SQLException
	{
		return false;
	}

	@Override
	public void moveToInsertRow() throws SQLException
	{
		throw new SQLException("not an updatable result set");
	}

	abstract protected Object readObject( int columnOrdinal )
			throws SQLException;

	@Override
	public void refreshRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowDeleted() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowInserted() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowUpdated() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchDirection( final int direction ) throws SQLException
	{
		switch (direction)
		{
			case ResultSet.FETCH_REVERSE:
			case ResultSet.FETCH_FORWARD:
				fetchDirection = direction;
				break;

			default:
				fetchDirection = ResultSet.FETCH_FORWARD;
		}
	}

	@Override
	public void setFetchSize( final int rows ) throws SQLException
	{
		// does nothing
	}

	@Override
	public <T> T unwrap( final Class<T> iface ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray( final int columnIndex, final Array x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray( final String columnLabel, final Array x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream( final int columnIndex, final InputStream x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream( final int columnIndex, final InputStream x,
			final int length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream( final int columnIndex, final InputStream x,
			final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream( final String columnLabel, final InputStream x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream( final String columnLabel,
			final InputStream x, final int length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream( final String columnLabel,
			final InputStream x, final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal( final int columnIndex, final BigDecimal x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal( final String columnLabel, final BigDecimal x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream( final int columnIndex, final InputStream x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream( final int columnIndex, final InputStream x,
			final int length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream( final int columnIndex, final InputStream x,
			final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream( final String columnLabel,
			final InputStream x ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream( final String columnLabel,
			final InputStream x, final int length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream( final String columnLabel,
			final InputStream x, final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob( final int columnIndex, final Blob x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob( final int columnIndex, final InputStream inputStream )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob( final int columnIndex,
			final InputStream inputStream, final long length )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob( final String columnLabel, final Blob x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob( final String columnLabel,
			final InputStream inputStream ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob( final String columnLabel,
			final InputStream inputStream, final long length )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean( final int columnIndex, final boolean x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean( final String columnLabel, final boolean x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte( final int columnIndex, final byte x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte( final String columnLabel, final byte x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes( final int columnIndex, final byte[] x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes( final String columnLabel, final byte[] x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream( final int columnIndex, final Reader x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream( final int columnIndex, final Reader x,
			final int length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream( final int columnIndex, final Reader x,
			final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream( final String columnLabel,
			final Reader reader ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream( final String columnLabel,
			final Reader reader, final int length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream( final String columnLabel,
			final Reader reader, final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob( final int columnIndex, final Clob x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob( final int columnIndex, final Reader reader )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob( final int columnIndex, final Reader reader,
			final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob( final String columnLabel, final Clob x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob( final String columnLabel, final Reader reader )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob( final String columnLabel, final Reader reader,
			final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate( final int columnIndex, final Date x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate( final String columnLabel, final Date x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble( final int columnIndex, final double x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble( final String columnLabel, final double x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat( final int columnIndex, final float x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat( final String columnLabel, final float x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt( final int columnIndex, final int x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt( final String columnLabel, final int x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong( final int columnIndex, final long x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong( final String columnLabel, final long x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream( final int columnIndex, final Reader x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream( final int columnIndex, final Reader x,
			final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream( final String columnLabel,
			final Reader reader ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream( final String columnLabel,
			final Reader reader, final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob( final int columnIndex, final NClob nClob )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob( final int columnIndex, final Reader reader )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob( final int columnIndex, final Reader reader,
			final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob( final String columnLabel, final NClob nClob )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob( final String columnLabel, final Reader reader )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob( final String columnLabel, final Reader reader,
			final long length ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString( final int columnIndex, final String nString )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString( final String columnLabel, final String nString )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull( final int columnIndex ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull( final String columnLabel ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject( final int columnIndex, final Object x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject( final int columnIndex, final Object x,
			final int scaleOrLength ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject( final String columnLabel, final Object x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject( final String columnLabel, final Object x,
			final int scaleOrLength ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef( final int columnIndex, final Ref x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef( final String columnLabel, final Ref x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId( final int columnIndex, final RowId x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId( final String columnLabel, final RowId x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort( final int columnIndex, final short x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort( final String columnLabel, final short x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML( final int columnIndex, final SQLXML xmlObject )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML( final String columnLabel, final SQLXML xmlObject )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString( final int columnIndex, final String x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString( final String columnLabel, final String x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime( final int columnIndex, final Time x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime( final String columnLabel, final Time x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp( final int columnIndex, final Timestamp x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp( final String columnLabel, final Timestamp x )
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean wasNull() throws SQLException
	{
		if (lastReadWasNull == null)
		{
			throw new SQLException(
					"Must read a column before calling wasNull()");
		}
		return lastReadWasNull;
	}

}
