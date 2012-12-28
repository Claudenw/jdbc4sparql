package org.xenei.jdbc4sparql.iface;

import java.util.Collection;
import java.util.Iterator;

public class NameFilter<T extends NamespacedObject> implements Iterator<T>, Iterable<T>
{
	String namePattern;
	Iterator<T> iter;
	T next;
	
	public NameFilter(String namePattern, Collection<T> objs)
	{
		this.namePattern = namePattern;
		iter = objs.iterator();
		next = null;
	}

	@Override
	public boolean hasNext()
	{
		if (namePattern == null)
		{
			return iter.hasNext();
		}
	
		while (next == null && iter.hasNext())
		{
			next = iter.next();
			if (!next.getLocalName().equals( namePattern ))
			{
				next = null;
			}
		}
		return next != null;
	}

	@Override
	public T next()
	{

		if (namePattern == null)
		{
			return iter.next();
		}
		else
		{
			T retval = next;
			next = null;
			return retval;
		}
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> iterator()
	{
		return this;
	}
	
	
}
