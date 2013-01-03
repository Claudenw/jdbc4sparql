package org.xenei.jdbc4sparql.iface;

import java.util.Comparator;

public class KeySegment implements Comparator<Object[]>
{
	private final int idx;
	private final boolean ascending;

	public KeySegment( final int idx, final ColumnDef columnDef )
	{
		this(idx, columnDef, true);
	}

	public KeySegment( final int idx, final ColumnDef columnDef,
			final boolean ascending )
	{
		this.idx = idx;
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

}
