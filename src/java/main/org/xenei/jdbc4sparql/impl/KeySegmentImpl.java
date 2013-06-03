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

import java.util.Comparator;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.TypeConverter;

public class KeySegmentImpl implements Comparator<Object[]>
{
	private final short idx;
	private final boolean ascending;

	public KeySegmentImpl( final int idx, final ColumnDef columnDef )
	{
		this(idx, columnDef, true);
	}

	public KeySegmentImpl( final int idx, final ColumnDef columnDef,
			final boolean ascending )
	{
		if (idx > Short.MAX_VALUE)
		{
			throw new IllegalArgumentException("Maximum index value is "
					+ Short.MAX_VALUE);
		}
		this.idx = (short) idx;
		this.ascending = ascending;
		final Class<?> type = TypeConverter.getJavaType(columnDef.getType());
		if (type == null)
		{
			throw new IllegalArgumentException(columnDef.getLabel()
					+ " uses an unsupported data type: " + columnDef.getType());
		}
		if ((type == null) || !Comparable.class.isAssignableFrom(type))
		{
			throw new IllegalArgumentException(columnDef.getLabel()
					+ " is not a comparable object type");
		}

	}

	@Override
	public int compare( final Object[] data1, final Object[] data2 )
	{
		final Object o1 = data1[idx];
		final Object o2 = data2[idx];
		int retval;
		if (o1 == null)
		{
			retval = o2 == null ? 0 : -1;
		}
		else if (o2 == null)
		{
			retval = 1;
		}
		else
		{
			retval = Comparable.class.cast(data1[idx]).compareTo(data2[idx]);
		}
		return ascending ? retval : -1 * retval;
	}

	public String getId()
	{
		return (ascending ? "A" : "D") + idx;
	}

	/**
	 * The index of the column position in the table (0 based)
	 * 
	 * @return the index.
	 */
	public short getIdx()
	{
		return idx;
	}

	public boolean isAscending()
	{
		return ascending;
	}
}
