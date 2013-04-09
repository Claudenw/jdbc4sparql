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
import java.util.Iterator;

import org.xenei.jdbc4sparql.iface.Table;

public abstract class IteratorResultSet extends AbstractResultSet
{
	Iterator<Object> rows;
	Object row = null;
	int position = 0;

	@SuppressWarnings( "unchecked" )
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
