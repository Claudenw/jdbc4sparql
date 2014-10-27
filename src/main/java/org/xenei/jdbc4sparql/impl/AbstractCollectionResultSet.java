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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;

import org.xenei.jdbc4sparql.iface.Table;

public abstract class AbstractCollectionResultSet extends AbstractResultSet
{
	private Collection<?> data;

	private int position;

	public AbstractCollectionResultSet( final Collection<?> rows,
			final Table table ) throws SQLException
	{
		super(table);
		position = -1;
		setTableData(rows);
	}

	@Override
	public boolean absolute( final int pos ) throws SQLException
	{
		if (pos < 0)
		{
			this.position = data.size() - pos - 1;
			;
		}
		else
		{
			this.position = pos - 1;
		}
		fixupPosition();
		return isValidPosition();
	}

	@Override
	public void afterLast() throws SQLException
	{

		position = data.size();
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

	protected void checkPosition() throws SQLException
	{
		if (!isValidPosition())
		{
			throw new SQLException("Cursor not positioned on a result");
		}
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
		final boolean retval = isValidPosition();
		fixupPosition();
		return retval;
	}

	/**
	 * Always called after a positioning change.
	 */
	protected void fixupPosition() throws SQLException
	{
		if (position < -1)
		{
			position = -1;
		}
		if (position > data.size())
		{
			position = data.size();
		}
	}

	protected Collection<?> getDataCollection()
	{
		return data;
	}

	protected int getPosition()
	{
		return position;
	}

	@Override
	public int getRow() throws SQLException
	{
		return position;
	}

	abstract protected Object getRowObject() throws SQLException;

	@Override
	public int getType() throws SQLException
	{
		return ResultSet.TYPE_SCROLL_SENSITIVE;
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		return position == data.size();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		return position == -1;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
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
		return position == (data.size() - 1);
	}

	private boolean isValidPosition()
	{
		return (position < data.size()) && (position > -1);
	}

	@Override
	public boolean isWrapperFor( final Class<?> iface ) throws SQLException
	{
		return false;
	}

	@Override
	public boolean last() throws SQLException
	{
		position = data.size() - 1;
		final boolean retval = isValidPosition();
		fixupPosition();
		return retval;
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		// do nothing
	}

	@Override
	public boolean next() throws SQLException
	{
		return relative(1);
	}

	@Override
	public boolean previous() throws SQLException
	{
		return relative(-1);
	}

	@Override
	public boolean relative( final int rows ) throws SQLException
	{
		switch (getFetchDirection())
		{
			case ResultSet.FETCH_REVERSE:
				position -= rows;
				break;
			case ResultSet.FETCH_FORWARD:
			default:
				position += rows;
				break;
		}
		final boolean retval = isValidPosition();
		fixupPosition();
		return retval;
	}

	protected void setTableData( final Collection<?> tableData )
			throws SQLException
	{
		this.data = tableData;
		fixupPosition();
	}

	@Override
	public <T> T unwrap( final Class<T> iface ) throws SQLException
	{
		return null;
	}
}
