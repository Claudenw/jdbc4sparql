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
