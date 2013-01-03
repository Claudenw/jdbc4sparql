package org.xenei.jdbc4sparql.impl;

import java.sql.SQLException;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Table;

public abstract class ListResultSet extends AbstractCollectionResultSet
{
	public ListResultSet( final List<?> rows, final Table table )
			throws SQLException
	{
		super(rows, table);
	}

	@Override
	protected Object getRowObject() throws SQLException
	{
		checkPosition();
		return ((List<?>) getDataCollection()).get(getPosition());
	}
}
