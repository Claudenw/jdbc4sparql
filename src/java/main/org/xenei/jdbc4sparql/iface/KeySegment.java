package org.xenei.jdbc4sparql.iface;

import java.util.Comparator;

public class KeySegment implements Comparator<Object[]>
{
	private int idx;
	private boolean ascending;
	
	public KeySegment( int idx, Column column, boolean ascending)
	{
		this.idx=idx;
		this.ascending=ascending;
		Class<?> type=TypeConverter.getJavaType(column.getType());
		if (type == null)
		{
			throw new IllegalArgumentException( column.getLabel()+" uses an unsupported data type: "+column.getType());
		}
		if (type == null || !Comparable.class.isAssignableFrom(type))
		{
			throw new IllegalArgumentException( column.getLabel()+" is not a comparable object type");
		}
		
	}
	
	public KeySegment(int idx, Column column)
	{
		this( idx, column, true );
	}
	
	public int compare( Object[] data1, Object[] data2)
	{
		Object o1 = data1[idx];
		Object o2 = data2[idx];
		int retval;
		if (o1 == null)
		{
			retval= o2==null?0:-1;
		}
		else if (o2 == null)
		{
			retval = 1;
		}
		else
		{
			retval = Comparable.class.cast(data1[idx]).compareTo( data2[idx] );
		}
		return ascending?retval:-1*retval;
	}
	
	
}
