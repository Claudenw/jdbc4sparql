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
import java.util.NavigableSet;
import java.util.SortedSet;

import org.apache.commons.collections.bag.AbstractMapBag;
import org.apache.commons.collections.bag.TreeBag;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;

public abstract class NavigableSetResultSet extends AbstractCollectionResultSet
{
	private Object currentObject;
	private Integer lastPosition;
	
	public NavigableSetResultSet( NavigableSet<? extends Object> rows, Table table ) throws SQLException
	{
		super( rows, table );
	}
	
	@Override
	public NavigableSet<Object> getDataCollection()
	{
		return (NavigableSet<Object>)super.getDataCollection();
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
			if ((getDataCollection().size()/2) > getPosition())
			{
				lastPosition = 0;
				currentObject = getDataCollection().first();
			}
			else
			{
				lastPosition = getDataCollection().size()-1;
				currentObject = getDataCollection().last();
			}
		}
		while (lastPosition>getPosition())
		{
			currentObject = getDataCollection().lower(currentObject);
			lastPosition--;
		}
		while (lastPosition<getPosition())
		{
			currentObject = getDataCollection().higher(currentObject);
			lastPosition++;
		}
	}
	
	@Override
	protected Object getRowObject() throws SQLException
	{
		if (currentObject == null)
		{
			throw new SQLException( "Result set has not been positioned");
		}
		return currentObject;
	}


}
