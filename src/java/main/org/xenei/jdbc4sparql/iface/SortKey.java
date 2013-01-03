package org.xenei.jdbc4sparql.iface;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortKey implements Comparator<Object[]>
{
	private boolean unique;
	private final List<KeySegment> segments;

	public SortKey()
	{
		segments = new ArrayList<KeySegment>();
	}

	public SortKey addSegment( final KeySegment segment )
	{
		segments.add(segment);
		return this;
	}

	@Override
	public int compare( final Object[] data1, final Object[] data2 )
	{
		for (final KeySegment segment : segments)
		{
			final int retval = segment.compare(data1, data2);
			if (retval != 0)
			{
				return retval;
			}
		}
		return 0;
	}

	public boolean isUnique()
	{
		return unique;
	}

	public void setUnique()
	{
		unique = true;
	}
}
