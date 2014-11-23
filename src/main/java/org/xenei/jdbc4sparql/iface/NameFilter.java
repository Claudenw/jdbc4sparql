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
package org.xenei.jdbc4sparql.iface;

import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.sparql.items.NamedObject;

/**
 * Filters a namespacedObject by name.
 *
 * @param <T>
 *            a NamespacedObject
 */
public class NameFilter<T extends NamedObject<?>> implements Iterator<T>,
		Iterable<T> {
	// the name pattern to match
	private String namePattern;
	// the iterator of the original collection.
	private Iterator<T> iter;
	// our next object.
	private T next;

	/**
	 * Construct a NameFilter from a pattern and a collection.
	 *
	 * If namePattern is null match all names.
	 *
	 * @param namePattern
	 *            The pattern to match or null.
	 * @param objs
	 *            The collection of objects to filter.
	 */
	public NameFilter(final String namePattern, final Collection<T> objs) {
		this(namePattern, objs.iterator());
	}

	/**
	 * Construct a NameFilter from a pattern and an iterator.
	 *
	 * If namePattern is null match all names.
	 *
	 * @param namePattern
	 *            The pattern to match or null.
	 * @param iter
	 *            the iterator of objects to filter.
	 */
	public NameFilter(final String namePattern, final Iterator<T> iter) {
		this.namePattern = namePattern;
		this.iter = iter;
		next = null;
	}

	@Override
	public boolean hasNext() {
		if (namePattern == null) {
			return iter.hasNext();
		}

		while ((next == null) && iter.hasNext()) {
			next = iter.next();
			if (!next.getName().getShortName().equals(namePattern)) {
				next = null;
			}
		}
		return next != null;
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}

	@Override
	public T next() {
		if (namePattern == null) {
			return iter.next();
		} else {
			final T retval = next;
			next = null;
			return retval;
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Return the data as a list.
	 *
	 * @return
	 */
	public List<T> toList() {
		return WrappedIterator.create(this).toList();
	}

	/**
	 * Return the data as a list.
	 *
	 * @return
	 */
	public Set<T> toSet() {
		return WrappedIterator.create(this).toSet();
	}
}
