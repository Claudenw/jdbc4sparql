package org.xenei.jdbc4sparql.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;

import org.xenei.jdbc4sparql.iface.Table;

public abstract class IteratorResultSet extends AbstractResultSet
{
	Iterator<Object> rows;
	Object row = null;
	int position = 0;

	public IteratorResultSet( final Iterator<?> rows, final Table table )
			throws SQLException
	{
		super(table);
		this.rows = (Iterator<Object>) rows;
	}

	@Override
	public boolean absolute( final int arg0 ) throws SQLException
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

	protected Object getRowObject() throws SQLException
	{
		if (row == null)
		{
			throw new SQLException("Cursor not positioned on a result");
		}
		return row;
	}

	@Override
	public int getType() throws SQLException
	{
		return ResultSet.TYPE_FORWARD_ONLY;
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
	public boolean relative( final int arg0 ) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}
}
