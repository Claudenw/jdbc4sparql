package org.xenei.jdbc4sparql.impl;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.xenei.jdbc4sparql.iface.Table;

public abstract class AbstractForwardReadableResultSet extends
		AbstractResultSet
{

	public AbstractForwardReadableResultSet( final Table table )
	{
		super(table);
	}

	@Override
	public boolean absolute( final int row ) throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public void afterLast() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public void beforeFirst() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public boolean first() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public int getType() throws SQLException
	{
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public boolean isFirst() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public boolean isLast() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public boolean last() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public boolean previous() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public void refreshRow() throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public boolean relative( final int rows ) throws SQLException
	{
		throw new SQLException("positioning not supported");
	}

	@Override
	public void setFetchDirection( final int direction ) throws SQLException
	{
		if (direction != ResultSet.FETCH_FORWARD)
		{
			throw new SQLException("positioning not supported");
		}
	}

}
