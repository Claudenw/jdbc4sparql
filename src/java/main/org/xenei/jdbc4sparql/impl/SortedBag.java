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

import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;


/**
 * A bag that is sorted by key.
 *
 * @param <T>
 */
public class SortedBag<T> implements Collection<T>
{
	private final TreeMap<T, List<T>> map;

	public SortedBag( final Comparator<T> comparator )
	{
		map = new TreeMap<T, List<T>>(comparator);
	}

	@Override
	public boolean add( final T e )
	{
		final List<T> lst = (map.containsKey(e)) ? map.get(e)
				: new ArrayList<T>();
		final boolean retval = lst.add(e);
		map.put(e, lst);
		return retval;
	}

	@Override
	public boolean addAll( final Collection<? extends T> c )
	{
		boolean retval = false;
		for (final T t : c)
		{
			retval |= add(t);
		}
		return retval;
	}

	@Override
	public void clear()
	{
		map.clear();
	}

	@Override
	public boolean contains( final Object o )
	{
		return map.containsKey(o);
	}

	@Override
	public boolean containsAll( final Collection<?> c )
	{
		for (final Object o : c)
		{
			if (!contains(o))
			{
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isEmpty()
	{
		return map.isEmpty();
	}

	@Override
	public Iterator<T> iterator()
	{
		ExtendedIterator<T> iter = NiceIterator.emptyIterator();
		for (final List<T> lst : map.values())
		{
			iter = iter.andThen(lst.iterator());
		}
		return iter;
		
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public boolean remove( final Object o )
	{
		boolean retval = false;
		final List<T> lst = map.get(o);
		if (lst != null)
		{
			retval = lst.contains(o);
			lst.remove(o);
			if (lst.size() == 0)
			{
				map.remove(o);
			}
			else
			{
				map.put((T) o, lst);
			}
		}
		return retval;
	}

	@Override
	public boolean removeAll( final Collection<?> c )
	{
		boolean retval = false;
		for (final Object o : c)
		{
			retval |= remove(o);
		}
		return retval;
	}

	@Override
	public boolean retainAll( final Collection<?> c )
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int size()
	{
		int size = 0;
		for (final List<T> l : map.values())
		{
			size += l.size();
		}
		return size;
	}

	@Override
	public Object[] toArray()
	{
		return ((ExtendedIterator<T>) iterator()).toList().toArray();
	}

	
	@SuppressWarnings( "unchecked" )
	@Override
	public <T> T[] toArray( T[] a )
	{
		return ((ExtendedIterator<T>) iterator()).toList().toArray(a);
	}
}
