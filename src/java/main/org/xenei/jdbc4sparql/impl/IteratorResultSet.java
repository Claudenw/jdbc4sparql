package org.xenei.jdbc4sparql.impl;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.bag.AbstractMapBag;
import org.apache.commons.collections.bag.TreeBag;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public abstract class IteratorResultSet extends AbstractResultSet
{
	Iterator<Object> rows;
	Object row = null;
	int position = 0;
	
	public IteratorResultSet( Iterator<?> rows, Table table ) throws SQLException
	{
		super( table );
		this.rows = (Iterator<Object>)rows;
	}

	protected Object getRowObject() throws SQLException
	{
		if (row == null)
		{
			throw new SQLException( "Cursor not positioned on a result");
		}
		return row;
	}

	@Override
	public boolean absolute( int arg0 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void afterLast() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void beforeFirst() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancelRowUpdates() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws SQLException
	{
		rows = null;
	}

	@Override
	public void deleteRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean first() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getRow() throws SQLException
	{
		return position;
	}

	@Override
	public int getType() throws SQLException
	{
		return 	ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return rows == null;
	}

	@Override
	public boolean isFirst() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isLast() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean last() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean next() throws SQLException
	{
		if (rows.hasNext())
		{
			position++;
			row = rows.next();
			return true;
		}
		row = null;
		return false;
	}

	@Override
	public boolean previous() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean relative( int arg0 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}
}
