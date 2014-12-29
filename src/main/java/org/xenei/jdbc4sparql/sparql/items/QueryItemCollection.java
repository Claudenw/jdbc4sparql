package org.xenei.jdbc4sparql.sparql.items;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.UniqueFilter;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * A collection of QueryItemInfo objects.
 *
 * locatable by
 * <ul>
 * <li>ItemName - uses match() to locate items.</li>
 * <li>NamedObject - checks for equality of baseObject.</li>
 * <li>QueryItemInfo - check for quality of objects</li>
 * </ul>
 *
 * @param <T>
 */
public class QueryItemCollection<I extends QueryItemInfo<T, N>, T extends NamedObject<N>, N extends ItemName>
		implements Collection<I> {

	private List<I> lst;

	public QueryItemCollection() {
		lst = new ArrayList<I>();
	}

	public QueryItemCollection(final Collection<? extends I> initial) {
		this();
		addAll(initial);
	}

	/**
	 * Add a QueryItemInfo to the collection
	 */
	@Override
	public boolean add(final I arg0) {
		if (contains(arg0)) {
			return false;
		}
		lst.add(arg0);
		return true;
	}

	/**
	 * Add all items in the collection.
	 */
	@Override
	public boolean addAll(final Collection<? extends I> arg0) {
		boolean retval = false;
		for (final I t : arg0) {
			retval |= add(t);
		}
		return retval;
	}

	/**
	 * clear all the entries from the collection.
	 */
	@Override
	public void clear() {
		lst.clear();
	}

	// public boolean contains(ItemName name) {
	// return match(name).hasNext();
	// }

	// public boolean contains(GUIDObject guidObj) {
	// return findGUID( guidObj ) != null;
	// }
	// public boolean contains(NamedObject<?> arg0) {
	// return match(arg0.getName()).hasNext();
	// }

	/**
	 * Executes one of the NamedObject, QueryItemInfo, and ItemName contains()
	 * methods or throws an IllegalArgumentException if not one of the above.
	 */
	@Override
	public boolean contains(final Object arg0) {
		if (arg0 instanceof QueryItemInfo<?, ?>) {
			return contains((QueryItemInfo<?, ?>) arg0);
		}
		if (arg0 instanceof NamedObject<?>) {
			return contains((NamedObject<?>) arg0);
		}
		if (arg0 instanceof ItemName) {
			return contains((ItemName) arg0);
		}
		throw new IllegalArgumentException(
				"Must be a QueryItemInfo, NamedObject, or ItemName");
	}

	/**
	 * Looks in the list for QueryItemInfo.equals( arg0 )
	 *
	 * @param arg0
	 *            the QueryItemInfo to look for.
	 * @return true if it is found.
	 */
	public boolean contains(final QueryItemInfo<?, ?> arg0) {
		return lst.contains(arg0);
	}

	/**
	 * Looks in the list for any QueryItemInfo.getBaseObject.equals();
	 *
	 * @param arg0
	 *            the Named object to look for
	 * @return true if the named object is found.
	 */
	public boolean contains(final NamedObject<?> arg0) {
		for (final QueryItemInfo<?, ?> itemInfo : lst) {
			if (itemInfo.getBaseObject().getName().matches(arg0.getName())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Looks in the list of any QueryItemInfo.match( ItemName )
	 *
	 * @param arg0
	 *            The ItemName to look for.
	 * @return true if the named object is found.
	 */
	public boolean contains(final ItemName arg0) {
		return match(arg0).hasNext();
	}

	/**
	 * performs contains on all of the objects in the collection.
	 */
	@Override
	public boolean containsAll(final Collection<?> arg0) {
		for (final Object o : arg0) {
			if (!contains(o)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * returns true if the list is empty.
	 */
	@Override
	public boolean isEmpty() {
		return lst.isEmpty();
	}

	/**
	 * Iterator over the QueryItemInfo
	 */
	@Override
	public ExtendedIterator<I> iterator() {
		return WrappedIterator.create(lst.iterator());
	}

	/**
	 * the Set of the unique the NamedObjects
	 */
	public Set<T> getNamedObjectSet() {
		return iterator().mapWith(new BaseObjectMap()).toSet();
	}

	/**
	 * Remove the QueryItemInfo from the list
	 *
	 * @param arg0
	 *            The QueryItemInfo to remove
	 * @return true if the item was removed.
	 */
	public boolean remove(final QueryItemInfo<?, ?> arg0) {
		return lst.remove(arg0);
	}

	/**
	 * Remove all matches for the ItemName.
	 *
	 * @param arg0
	 *            the ItemName to remove.
	 * @return true if an item has been removed.
	 */
	public boolean remove(final ItemName arg0) {
		final int startSize = lst.size();
		lst = notMatch(arg0).toList();
		return startSize != lst.size();
	}

	/**
	 * Remove all items that have the NamedObject as the baseObject.
	 *
	 * @param arg0
	 *            the NamedObject to remove.
	 * @return true if any objects were removed.
	 */
	public boolean remove(final NamedObject<?> arg0) {
		boolean found = false;
		for (final QueryItemInfo<?, ?> itemInfo : lst) {
			if (itemInfo.getBaseObject().equals(arg0)) {
				found |= remove(itemInfo);
			}
		}
		return found;
	}

	/**
	 * remove the object if it is a QueryItemInfo, NamedObject or ItemName.
	 *
	 */
	@Override
	public boolean remove(final Object arg0) {
		if (arg0 instanceof QueryItemInfo<?, ?>) {
			return remove((QueryItemInfo<?, ?>) arg0);
		}
		if (arg0 instanceof NamedObject<?>) {
			return remove((NamedObject<?>) arg0);
		}
		if (arg0 instanceof ItemName) {
			return remove((ItemName) arg0);
		}
		throw new IllegalArgumentException(
				"Must be a QueryItemInfo, NamedObject, or ItemName");
	}

	/**
	 * removes all the objects in the collection if they are a QueryItemInfo,
	 * NamedObject or ItemName.
	 *
	 */
	@Override
	public boolean removeAll(final Collection<?> arg0) {
		boolean retval = false;
		for (final Object o : arg0) {
			retval |= remove(o);
		}
		return retval;
	}

	/**
	 * retains all the objects in the collection if they are a QueryItemInfo,
	 * NamedObject or ItemName.
	 *
	 */
	@Override
	public boolean retainAll(final Collection<?> arg0) {
		if (arg0.isEmpty()) {
			return false;
		}
		final Object o = arg0.iterator().next();
		if (o instanceof QueryItemInfo<?, ?>) {
			return lst.retainAll(arg0);
		}

		if (o instanceof NamedObject<?>) {
			return retainAllNamedObject((Collection<NamedObject<?>>) arg0);
		}
		if (o instanceof ItemName) {
			return retainAllItemName((Collection<ItemName>) arg0);
		}
		throw new IllegalArgumentException(
				"Must be a collection of QueryItemInfo, NamedObject, or ItemName");
	}

	private boolean retainAllNamedObject(final Collection<NamedObject<?>> arg0) {
		final List<ItemName> tmpLst = WrappedIterator.create(arg0.iterator())
				.mapWith(new Map1<NamedObject<?>, ItemName>() {

					@Override
					public ItemName map1(final NamedObject<?> o) {
						return o.getName();
					}
				}).toList();
		return retainAllItemName(tmpLst);
	}

	private boolean retainAllItemName(final Collection<ItemName> arg0) {
		boolean retval = false;
		final List<I> newLst = new ArrayList<I>();
		final List<I> dupLst = new ArrayList<I>();
		I itemInfo;
		dupLst.addAll(lst);
		final Iterator<ItemName> outer = WrappedIterator
				.create(arg0.iterator()).filterKeep(
						new UniqueFilter<ItemName>());
		ItemName name;
		while (outer.hasNext()) {
			name = outer.next();
			final Iterator<I> iter = dupLst.iterator();
			while (iter.hasNext()) {
				itemInfo = iter.next();
				if (itemInfo.getBaseObject().getName().matches(name)) {
					newLst.add(itemInfo);
					iter.remove();
					retval = true;
				}
			}
		}
		if (retval) {
			lst = newLst;
		}
		return retval;
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
	public <T2> T2[] toArray(final T2[] arg0) {
		return lst.toArray(arg0);
	}

	/**
	 * Get the object with the specified name using the match algorithm.
	 *
	 * @param name
	 *            The item name to find.
	 * @return The object for the name or null if none found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public I get(final ItemName name) {
		final Iterator<I> iter = match(name);
		if (iter.hasNext()) {
			final I retval = iter.next();
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
	 * Get the object with the specified name using the match algorithm.
	 *
	 * @param namedObject
	 *            The NamedObject to find.
	 * @return The object for the name or null if none found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public I get(final T namedObject) {
		final Iterator<I> iter = match(namedObject);
		if (match(namedObject).hasNext()) {
			final I retval = iter.next();
			if (iter.hasNext()) {
				throw new IllegalArgumentException(String.format(
						SparqlQueryBuilder.FOUND_IN_MULTIPLE_, namedObject, lst
								.get(0).getClass()));
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
	public I get(final int i) throws IndexOutOfBoundsException {
		return lst.get(i);
	}

	// public I findGUID(GUIDObject name) {
	// NamedObjectGUIDFilter<I> nof = new NamedObjectGUIDFilter<I>(name);
	// ExtendedIterator<I> iter = iterator().filterKeep(nof);
	// if (iter.hasNext()) {
	// return iter.next();
	// }
	// return null;
	//
	// }
	//
	public I findGUIDVar(final String guid) {

		final ExtendedIterator<I> iter = iterator();
		I backupResult = null;
		while (iter.hasNext()) {
			final I item = iter.next();
			if (item.getName().getGUID().equals(guid)) {
				return item;
			}
			if ((backupResult == null) && item.getGUID().equals(guid)) {
				backupResult = item;
			}
		}

		return backupResult;

	}

	public int count(final ItemName name) {
		return match(name).toList().size();
	}

	public int count(final T namedObject) {
		return match(namedObject).toList().size();
	}

	public ExtendedIterator<I> match(final ItemName name) {
		final ItemNameFilter<I> nof = new ItemNameFilter<I>(name);
		return iterator().filterKeep(nof);
	}

	public ExtendedIterator<I> match(final T name) {
		final NamedObjectFilter<I> nof = new NamedObjectFilter<I>(name);
		return iterator().filterKeep(nof);
	}

	public ExtendedIterator<I> findBaseObject(final T baseObject) {
		return iterator().filterKeep(new BaseObjectFilter<I>(baseObject));
	}

	public ExtendedIterator<I> notMatch(final ItemName name) {
		final ItemNameFilter<I> nof = new ItemNameFilter<I>(name);
		return iterator().filterDrop(nof);
	}

	public ExtendedIterator<I> notMatch(final T namedObject) {
		final NamedObjectFilter<I> nof = new NamedObjectFilter<I>(namedObject);
		return iterator().filterDrop(nof);
	}

	public int indexOf(final QueryItemInfo<?, ?> item) {
		return lst.indexOf(item);
	}

	public int indexOf(final T namedObject) {
		final NamedObjectFilter<I> nof = new NamedObjectFilter<I>(namedObject);
		final Iterator<I> iter = iterator();
		int i = 0;
		while (iter.hasNext()) {
			if (nof.accept(iter.next())) {
				return i;
			}
			i++;
		}
		return -1;
	}

	public int indexOf(final ItemName name) {
		final ItemNameFilter<I> nof = new ItemNameFilter<I>(name);
		final Iterator<I> iter = iterator();
		int i = 0;
		while (iter.hasNext()) {
			if (nof.accept(iter.next())) {
				return i;
			}
			i++;
		}
		return -1;
	}

	@Override
	public String toString() {
		return lst.toString();
	}

	public static class ItemNameFilter<I extends QueryItemInfo<?, ?>> extends
			Filter<I> {

		protected Collection<ItemName> others;

		public ItemNameFilter(final ItemName other) {
			this.others = new ArrayList<ItemName>();
			this.others.add(other);
		}

		public ItemNameFilter(final Collection<?> others) {
			this.others = new ArrayList<ItemName>();
			for (final Object t : others) {
				if (t instanceof ItemName) {
					this.others.add((ItemName) t);
				}
				else if (t instanceof NamedObject) {
					this.others.add(((NamedObject<?>) t).getName());
				}
				else {
					throw new IllegalArgumentException(String.format(
							"%s is not an instance of ItemName or NamedObject",
							t.getClass()));
				}
			}
		}

		@Override
		public boolean accept(final I item) {
			final ItemName name = item.getName().clone(NameSegments.ALL);
			for (final ItemName other : others) {
				if (other.matches(name)) {
					return true;
				}
			}
			return false;
		}

	}

	public class BaseObjectMap implements Map1<I, T> {
		@Override
		public T map1(final I o) {
			return o.getBaseObject();
		}
	}

	public class BaseObjectFilter<I extends QueryItemInfo<?, ?>> extends
			Filter<I> {

		protected Collection<T> others;

		public BaseObjectFilter(final T other) {
			this.others = new ArrayList<T>();
			this.others.add(other);
		}

		public BaseObjectFilter(final Collection<?> others) {
			this.others = new ArrayList<T>();
			for (final Object t : others) {
				if (t instanceof NamedObject<?>) {
					this.others.add((T) t);
				}
				else {
					throw new IllegalArgumentException(String.format(
							"%s is not an instance of ItemName or NamedObject",
							t.getClass()));
				}
			}
		}

		@Override
		public boolean accept(final I item) {
			for (final T other : others) {
				if (other.getName().getGUID()
						.equals(item.getBaseObject().getName().getGUID())) {
					return true;
				}
			}
			return false;
		}

	}
}
