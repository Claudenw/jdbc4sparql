package org.xenei.jdbc4sparql.sparql.items;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.name.GUIDObject;
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
public class QueryItemCollection<I extends QueryItemInfo<T,N>, T extends NamedObject<N>, N extends ItemName> implements
		Collection<I> {

	private List<I> lst;

	public QueryItemCollection() {
		lst = new ArrayList<I>();
	}
	
	public QueryItemCollection( Collection<? extends I> initial )
	{
		this();
		addAll( initial );
	}

	/**
	 * Add a QueryItemInfo to the collection
	 */
	@Override
	public boolean add(I arg0) {
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
	public boolean addAll(Collection<? extends I> arg0) {
		boolean retval = false;
		for (I t : arg0) {
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

	

//	public boolean contains(ItemName name) {
//		return match(name).hasNext();
//	}

//	public boolean contains(GUIDObject guidObj) {
//		return findGUID( guidObj ) != null;
//	}
//	public boolean contains(NamedObject<?> arg0) {
//		return match(arg0.getName()).hasNext();
//	}

	/**
	 * Executes one of the NamedObject, QueryItemInfo, and ItemName contains() methods or 
	 * throws an IllegalArgumentException if not one of the above.
	 */
	@Override
	public boolean contains(Object arg0) {
		if (arg0 instanceof QueryItemInfo<?,?>) {
			return contains((QueryItemInfo<?,?>) arg0);
		}
		if (arg0 instanceof NamedObject<?>) {
			return contains((NamedObject<?>) arg0);
		}
		if (arg0 instanceof ItemName) {
			return contains((ItemName) arg0);
		}
		throw new IllegalArgumentException( "Must be a QueryItemInfo, NamedObject, or ItemName");
	}

	/**
	 * Looks in the list for QueryItemInfo.equals( arg0 )
	 * @param arg0 the QueryItemInfo to look for.
	 * @return true if it is found.
	 */
	public boolean contains(QueryItemInfo<?,?> arg0) {
			return lst.contains((QueryItemInfo<?,?>) arg0);
		}
	
	/**
	 * Looks in the list for any QueryItemInfo.getBaseObject.equals();
	 * @param arg0 the Named object to look for
	 * @return true if the named object is found.
	 */
	public boolean contains(NamedObject<?> arg0) {
		for (QueryItemInfo<?,?> itemInfo : lst )
		{
			if (itemInfo.getBaseObject().equals(arg0))
			{
				return true;
			}
		}
		return false;
		}
	
	/**
	 * Looks in the list of any QueryItemInfo.match( ItemName )
	 * @param arg0 The ItemName to look for.
	 * @return true if the named object is found.
	 */
	public boolean contains(ItemName arg0) {
		return match( arg0 ).hasNext();
	}
	
	/**
	 * performs contains on all of the  objects in the collection.
	 */
	@Override
	public boolean containsAll(Collection<?> arg0) {
		for (Object o : arg0) {
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
	public Set<T> setNamedObject() {
		return iterator().mapWith( new Map1<I,T>(){

			@Override
			public T map1(I o) {
				return o.getBaseObject();
			}}).toSet();
	}
	
//	/**
//	 * Return a mapped iterator
//	 * @param map A map that converts the QueryItemInfo into another object type.
//	 * @return An iterator on the object.
//	 */
//	public <X> ExtendedIterator<X> iterator(Map1<I, X> map) {
//		return iterator().mapWith(map);
//	}

	/**
	 * Remove the QueryItemInfo from the list
	 * @param arg0 The QueryItemInfo to remove
	 * @return true if the item was removed.
	 */
	public boolean remove(QueryItemInfo<?,?> arg0) {
		return lst.remove( arg0 );
	}

	/**
	 * Remove all matches for the ItemName.
	 * @param arg0 the ItemName to remove.
	 * @return true if an item has been removed.
	 */
	public boolean remove(ItemName arg0) {
		int startSize = lst.size();
		lst = notMatch(arg0).toList();
		return startSize != lst.size();
	}
	
	/**
	 * Remove all items that have the NamedObject as the baseObject.
	 * @param arg0 the NamedObject to remove.
	 * @return true if any objects were removed.
	 */
	public boolean remove(NamedObject<?> arg0) {
		boolean found = false;
		for (QueryItemInfo<?,?> itemInfo : lst )
		{
			if (itemInfo.getBaseObject().equals(arg0))
			{
				found |= remove( itemInfo );
			}
		}
		return found;
	}

	/**
	 * remove the object if it is a QueryItemInfo, NamedObject or ItemName.
	 * 
	 */
	@Override
	public boolean remove(Object arg0) {
		if (arg0 instanceof QueryItemInfo<?,?>) {
			return remove((QueryItemInfo<?,?>) arg0);
		}
		if (arg0 instanceof NamedObject<?>) {
			return remove((NamedObject<?>) arg0);
		}
		if (arg0 instanceof ItemName) {
			return remove((ItemName) arg0);
		}
		throw new IllegalArgumentException( "Must be a QueryItemInfo, NamedObject, or ItemName");
	}

	/**
	 * removes all the objects in the collection  if they are a QueryItemInfo, NamedObject or ItemName.
	 * 
	 */
	@Override
	public boolean removeAll(Collection<?> arg0) {
		boolean retval = false;
		for (Object o : arg0)
		{
			retval |= remove( o );
		}
		return retval;
	}

	/**
	 * retains all the objects in the collection  if they are a QueryItemInfo, NamedObject or ItemName.
	 * 
	 */
	@Override
	public boolean retainAll(Collection<?> arg0) {
		if (arg0 instanceof QueryItemInfo<?,?>) {
			return lst.retainAll( arg0);
		}
		if (arg0 instanceof NamedObject<?>) {
			int count  = lst.size();
			Set<I> s = new HashSet<I>();
			for (QueryItemInfo<T,N> itemInfo : lst)
			{
				s.addAll(match( (T)itemInfo.getBaseObject() ).toSet());
			}
			lst = new ArrayList<I>();
			lst.addAll( s );
			return count - lst.size() > 0;
		}
		if (arg0 instanceof ItemName) {
			int count  = lst.size();
			Set<I> s = new HashSet<I>();
			for (QueryItemInfo<?,?> itemInfo : lst)
			{
				s.addAll(match( itemInfo.getName() ).toSet());
			}
			lst = new ArrayList<I>();
			lst.addAll( s );
			return count - lst.size() > 0;
		}
		throw new IllegalArgumentException( "Must be a QueryItemInfo, NamedObject, or ItemName");
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
	 *            The item name to find.
	 * @return The object for the name or null if none found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public I get(ItemName name) {
		Iterator<I> iter = match(name);
		if (iter.hasNext()) {
			I retval = iter.next();
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
	public I get(T namedObject) {
		Iterator<I> iter = match(namedObject);
		if (match(namedObject).hasNext()) {
			I retval = iter.next();
			if (iter.hasNext()) {
				throw new IllegalArgumentException(String.format(
						SparqlQueryBuilder.FOUND_IN_MULTIPLE_, namedObject, lst.get(0)
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
	public I get(int i) throws IndexOutOfBoundsException {
		return lst.get(i);
	}

//	public I findGUID(GUIDObject name) {
//		NamedObjectGUIDFilter<I> nof = new NamedObjectGUIDFilter<I>(name);
//		ExtendedIterator<I> iter = iterator().filterKeep(nof);
//		if (iter.hasNext()) {
//			return iter.next();
//		}
//		return null;
//
//	}
//
//	public I findGUID(String name) {
//		NamedObjectGUIDFilter<I> nof = new NamedObjectGUIDFilter<I>(name);
//		ExtendedIterator<I> iter = iterator().filterKeep(nof);
//		if (iter.hasNext()) {
//			return iter.next();
//		}
//		return null;
//
//	}

	public int count(ItemName name) {
		return match(name).toList().size();
	}
	
	public int count(T namedObject) {
		return match(namedObject).toList().size();
	}
	
	public ExtendedIterator<I> match(ItemName name) {
		ItemNameFilter<I> nof = new ItemNameFilter<I>(name);
		return iterator().filterKeep(nof);
	}

	public ExtendedIterator<I> match(T name) {
		NamedObjectFilter<I> nof = new NamedObjectFilter<I>(name);
		return iterator().filterKeep(nof);
	}
	
//	public ExtendedIterator<I> match(Collection<?> arg0) {
//		NamedObjectFilter<I> nof = new NamedObjectFilter<I>(arg0);
//		return iterator().filterKeep(nof);
//	}

	public ExtendedIterator<I> notMatch(ItemName name) {
		ItemNameFilter<I> nof = new ItemNameFilter<I>(name);
		return iterator().filterDrop(nof);
	}

	public ExtendedIterator<I> notMatch(T namedObject) {
		NamedObjectFilter<I> nof = new NamedObjectFilter<I>(namedObject);
		return iterator().filterDrop(nof);
	}
	
//	public ExtendedIterator<I> notMatch(Collection<?> arg0) {
//		Iterator<?> iter = arg0.iterator();
//		if (iter.hasNext())
//		{
//			Filter<I> fltr = null;
//			Object o = iter.next();
//			if (o instanceof ItemName)
//			{
//				fltr = new ItemNameFilter<I>(arg0);
//			}
//			else if (o instanceof NamedObject)
//			{
//				fltr = new NamedObjectFilter<I>(arg0); 
//			} else {
//				throw new IllegalArgumentException( "Argument but be a collection of ItemName or NamedObject");
//			}
//			return iterator().filterDrop(fltr);
//		}
//		return WrappedIterator.emptyIterator();
//	}

	public int indexOf(QueryItemInfo<?,?> item)
	{
		return lst.indexOf(item);
	}
	
	public int indexOf(T namedObject) {
		NamedObjectFilter<I> nof = new NamedObjectFilter<I>(namedObject);
		Iterator<I> iter = iterator();
		int i = 0;
		while (iter.hasNext()) {
			if (nof.accept(iter.next())) {
				return i;
			}
			i++;
		}
		return -1;
	}
	
	public int indexOf(ItemName name) {
		ItemNameFilter<I> nof = new ItemNameFilter<I>(name);
		Iterator<I> iter = iterator();
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
	public String toString()
	{
		return lst.toString();
	}

	public static class ItemNameFilter<I extends QueryItemInfo<?,?>> extends Filter<I> {

		protected Collection<ItemName> others;

		public ItemNameFilter(ItemName other) {
			this.others = new ArrayList<ItemName>();
			this.others.add(other);
		}

		public ItemNameFilter(Collection<?> others) {
			this.others = new ArrayList<ItemName>();
			for (Object t : others) {
				if (t instanceof ItemName) {
					this.others.add((ItemName) t);
				} else if (t instanceof NamedObject) {
					this.others.add(((NamedObject<?>) t).getName());
				} else {
					throw new IllegalArgumentException(String.format(
							"%s is not an instance of ItemName or NamedObject",
							t.getClass()));
				}
			}
		}

		@Override
		public boolean accept(I item) {
			ItemName name = item.getName().clone(NameSegments.ALL);
			for (ItemName other : others) {
				if (other.matches(name)) {
					return true;
				}
			}
			return false;
		}

	}
	
	public class NamedObjectFilter<I extends QueryItemInfo<?,?>> extends Filter<I> {

		protected Collection<T> others;

		public NamedObjectFilter(T other) {
			this.others = new ArrayList<T>();
			this.others.add(other);
		}

		public NamedObjectFilter(Collection<?> others) {
			this.others = new ArrayList<T>();
			for (Object t : others) {
			   if (t instanceof NamedObject<?>) {
					this.others.add((T) t);
				} else {
					throw new IllegalArgumentException(String.format(
							"%s is not an instance of ItemName or NamedObject",
							t.getClass()));
				}
			}
		}

		@Override
		public boolean accept(I item) {
			for (T other : others) {
				if (other.equals(item.getBaseObject())) {
					return true;
				}
			}
			return false;
		}

	}

}
