package org.xenei.jdbc4sparql.iface;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortKey implements Comparator<Object[]>
{
	private boolean unique;
	private List<KeySegment> segments;
	
	public void setUnique()
	{
		unique = true;
	}
	
	public boolean isUnique()
	{
		return unique;
	}
	
	public SortKey()
	{
		segments = new ArrayList<KeySegment>();
	}
	
	public SortKey addSegment(KeySegment segment) {
		segments.add(segment);
		return this;
	}

	@Override
	public int compare( Object[] data1, Object[] data2 )
	{
		for (KeySegment segment : segments)
		{
			int retval =segment.compare(data1, data2);
			if (retval != 0)
			{
				return retval;
			}
		}
		return 0;
	}
}
