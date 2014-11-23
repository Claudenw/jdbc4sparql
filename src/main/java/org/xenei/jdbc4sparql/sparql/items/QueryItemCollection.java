package org.xenei.jdbc4sparql.sparql.items;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class QueryItemCollection<T extends QueryItemInfo<?>> implements
		Collection<T> {

	private List<T> lst;

	public QueryItemCollection() {
		lst = new ArrayList<T>();
	}

	@Override
	public boolean add(T arg0) {
		if (contains(arg0)) {
			return false;
		}
		lst.add(arg0);
		return true;
	}

	@Override
	public boolean addAll(Collection<? extends T> arg0) {
		boolean retval = false;
		for (T t : arg0) {
			retval |= add(t);
		}
		return retval;
	}

	@Override
	public void clear() {
		lst.clear();
	}

	/**
	 * Contains by name
	 */

	public boolean contains(ItemName name) {
		return match(name).hasNext();
	}

	public boolean contains(NamedObject<?> arg0) {
		return match(arg0.getName()).hasNext();
	}

	@Override
	public boolean contains(Object arg0) {
		if (arg0 instanceof ItemName) {
			return contains((ItemName) arg0);
		}
		return contains((NamedObject<?>) arg0);
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		for (Object o : arg0) {
			if (!contains(o)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isEmpty() {
		return lst.isEmpty();
	}

	@Override
	public ExtendedIterator<T> iterator() {
		return WrappedIterator.create(lst.iterator());
	}

	public <X> ExtendedIterator<X> iterator(Map1<T, X> map) {
		return iterator().mapWith(map);
	}

	public boolean remove(QueryItemInfo arg0) {
		int startSize = lst.size();
		lst = notMatch(arg0.getName()).toList();
		return startSize != lst.size();
	}

	public boolean remove(ItemName arg0) {
		int startSize = lst.size();
		lst = notMatch(arg0).toList();
		return startSize != lst.size();
	}

	@Override
	public boolean remove(Object arg0) {
		int startSize = lst.size();
		lst = notMatch((ItemName) arg0).toList();
		return startSize != lst.size();
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {
		int startSize = lst.size();
		lst = notMatch(arg0).toList();
		return (startSize - arg0.size()) == lst.size();
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
		int startSize = lst.size();
		lst = match(arg0).toList();
		return startSize != lst.size();
	}

	@Override
	public int size() {
		return lst.size();
	}

	@Override
	public Object[] toArray() {
		return lst.toArray();
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		return lst.toArray(arg0);
	}

	/**
	 * Get the object with the specified name using the match algorithm.
	 *
	 * @param name
	 *            The table name to find.
	 * @return The object for the name or null if none found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public T get(ItemName name) {
		Iterator<T> iter = match(name);
		if (match(name).hasNext()) {
			T retval = iter.next();
			if (iter.hasNext()) {
				throw new IllegalArgumentException(String.format(
						SparqlQueryBuilder.FOUND_IN_MULTIPLE_, name, lst.get(0)
								.getClass()));
			}
			return retval;
		}
		return null;
	}

	/**
	 * Return the object at position i.
	 * 
	 * @param i
	 *            the index (0 based) of the object to return.
	 * @return the object.
	 * @throws IndexOutIndexOutOfBoundsException
	 *             if i<0 or i>=size();
	 */
	public T get(int i) throws IndexOutOfBoundsException {
		return lst.get(i);
	}

	/**
	 * Return object associated with node or null if not found.
	 * 
	 * The string used to create the node must use the same segments as the
	 * items in the list.
	 * 
	 * @param node
	 *            the node to look for.
	 * @return object or null if not found.
	 */
	public T get(Node node) {
		QueryItemNodeFilter<T> nof = new QueryItemNodeFilter<T>(node);
		Iterator<T> iter = iterator().filterKeep(nof);
		if (iter.hasNext()) {
			return iter.next();
		}
		return null;
	}

	public T findGUID(GUIDObject name) {
		NamedObjectGUIDFilter<T> nof = new NamedObjectGUIDFilter<T>(name);
		Iterator<T> iter = iterator().filterKeep(nof);
		if (iter.hasNext()) {
			return iter.next();
		}
		return null;

	}

	public T findGUID(String name) {
		NamedObjectGUIDFilter<T> nof = new NamedObjectGUIDFilter<T>(name);
		Iterator<T> iter = iterator().filterKeep(nof);
		if (iter.hasNext()) {
			return iter.next();
		}
		return null;

	}

	public int count(ItemName name) {
		return match(name).toList().size();
	}

	public ExtendedIterator<T> match(ItemName name) {
		NamedObjectFilter<T> nof = new NamedObjectFilter<T>(name);
		return iterator().filterKeep(nof);
	}

	public ExtendedIterator<T> match(Collection<?> arg0) {
		NamedObjectFilter<T> nof = new NamedObjectFilter<T>(arg0);
		return iterator().filterKeep(nof);
	}

	public ExtendedIterator<T> notMatch(ItemName name) {
		NamedObjectFilter<T> nof = new NamedObjectFilter<T>(name);
		return iterator().filterDrop(nof);
	}

	public ExtendedIterator<T> notMatch(Collection<?> arg0) {
		NamedObjectFilter<T> nof = new NamedObjectFilter<T>(arg0);
		return iterator().filterDrop(nof);
	}

	public int indexOf(ItemName name) {
		NamedObjectFilter<T> nof = new NamedObjectFilter<T>(name);
		Iterator<T> iter = iterator();
		int i = 0;
		while (iter.hasNext()) {
			if (nof.accept(iter.next())) {
				return i;
			}
			i++;
		}
		return -1;
	}
}
