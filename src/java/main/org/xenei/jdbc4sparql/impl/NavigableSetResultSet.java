package org.xenei.jdbc4sparql.impl;

import java.sql.SQLException;
import java.util.NavigableSet;

import org.xenei.jdbc4sparql.iface.Table;

public abstract class NavigableSetResultSet extends AbstractCollectionResultSet
{
	private Object currentObject;
	private Integer lastPosition;

	public NavigableSetResultSet( final NavigableSet<? extends Object> rows,
			final Table table ) throws SQLException
	{
		super(rows, table);
	}

	@Override
	protected void fixupPosition() throws SQLException
	{
		super.fixupPosition();
		if (isBeforeFirst() || isAfterLast())
		{
			lastPosition = getPosition();
			currentObject = null;
			return;
		}
		if (isFirst())
		{
			lastPosition = getPosition();
			currentObject = getDataCollection().first();
			return;
		}
		if (isLast())
		{
			lastPosition = getPosition();
			currentObject = getDataCollection().last();
			return;
		}
		if (lastPosition == null)
		{
			if ((getDataCollection().size() / 2) > getPosition())
			{
				lastPosition = 0;
				currentObject = getDataCollection().first();
			}
			else
			{
				lastPosition = getDataCollection().size() - 1;
				currentObject = getDataCollection().last();
			}
		}
		while (lastPosition > getPosition())
		{
			currentObject = getDataCollection().lower(currentObject);
			lastPosition--;
		}
		while (lastPosition < getPosition())
		{
			currentObject = getDataCollection().higher(currentObject);
			lastPosition++;
		}
	}

	@Override
	public NavigableSet<Object> getDataCollection()
	{
		return (NavigableSet<Object>) super.getDataCollection();
	}

	@Override
	protected Object getRowObject() throws SQLException
	{
		if (currentObject == null)
		{
			throw new SQLException("Result set has not been positioned");
		}
		return currentObject;
	}

}
