package org.xenei.jdbc4sparql.iface;

import java.util.Collection;
import java.util.Iterator;

public class NameFilter<T extends NamespacedObject> implements Iterator<T>,
		Iterable<T>
{
	String namePattern;
	Iterator<? extends T> iter;
	T next;

	public NameFilter( final String namePattern, final Collection<T> objs )
	{
		this(namePattern, objs.iterator());
	}

	public NameFilter( final String namePattern,
			final Iterator<? extends T> iter )
	{
		this.namePattern = namePattern;
		this.iter = iter;
		next = null;
	}

	@Override
	public boolean hasNext()
	{
		if (namePattern == null)
		{
			return iter.hasNext();
		}

		while ((next == null) && iter.hasNext())
		{
			next = iter.next();
			if (!next.getLocalName().equals(namePattern))
			{
				next = null;
			}
		}
		return next != null;
	}

	@Override
	public Iterator<T> iterator()
	{
		return this;
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
			final T retval = next;
			next = null;
			return retval;
		}
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

}
