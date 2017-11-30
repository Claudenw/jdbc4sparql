package org.xenei.jdbc4sparql.iface.name;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.jdbc4sparql.iface.NameSegments;

public class ItemNameMatcher {

	// single value searches
	
	public static <T extends ItemName> ExtendedIterator<T> match(Collection<T> items, final T name, NameSegments segs) {
		return WrappedIterator.create(items.iterator()).filterKeep(new ItemNameFilter<T>(name, segs));
	}
	
	public static <T extends ItemName> ExtendedIterator<T> notMatch(Collection<T> items, final T name, NameSegments segs) {
		return WrappedIterator.create(items.iterator()).filterDrop(new ItemNameFilter<T>(name, segs));
	}
	
	public static <T extends ItemName>  boolean contains(Collection<T> items, final T name, NameSegments segs) {
		return match( items, name, segs).hasNext();
	}
	
	public static <T extends ItemName> long count(Collection<T> items, final T name, NameSegments segs) {
		Long count=items.stream().filter(new ItemNameFilter<T>(name, segs)).collect(Collectors.counting());
		return count;
	}
	
	public static <T extends ItemName> T get(Collection<T> items, final T name, NameSegments segs) {
		Optional<T> anser = items.stream().filter( new ItemNameFilter<T>(name, segs)).findFirst();
		return anser.isPresent()?anser.get():null;
	}
	
	public static <T extends ItemName> T get(Collection<T> items, final String guid) {
		Optional<T> anser = items.stream().filter( i -> i.getGUID().equals(guid)).findFirst();
		return anser.isPresent()?anser.get():null;
	}
	
	public static <T extends ItemName> boolean remove(Collection<T> items, final T name, NameSegments segs) {
		boolean retval = false;
		Iterator<T> iter = WrappedIterator.create(items.iterator()).filterKeep(new ItemNameFilter<T>(name, segs));
		while (iter.hasNext())
		{
			iter.remove();
			retval = true;
		}
		return retval;
	}
	
	// multi value searches
	
	public static <T extends ItemName> ExtendedIterator<T> match(Collection<T> items, final Collection<T> names, NameSegments segs) {
		return WrappedIterator.create(items.iterator()).filterKeep(new ItemNameFilter<T>(names, segs));
	}
	
	public static <T extends ItemName> ExtendedIterator<T> notMatch(Collection<T> items, final Collection<T> names, NameSegments segs) {
		return WrappedIterator.create(items.iterator()).filterDrop(new ItemNameFilter<T>(names, segs));
	}
	
	public static <T extends ItemName> boolean contains(Collection<T> items, final Collection<T> names, NameSegments segs) {
		return match( items, names, segs).hasNext();
	}
	
	public static <T extends ItemName> long count(Collection<T> items, final Collection<T> names, NameSegments segs) {
		Long count=items.stream().filter(new ItemNameFilter<T>(names, segs)).collect(Collectors.counting());
		return count;
	}
	
	public static <T extends ItemName> T get(Collection<T> items, final Collection<T> names, NameSegments segs) {
		Optional<T> anser = items.stream().filter( new ItemNameFilter<T>(names, segs)).findFirst();
		return anser.isPresent()?anser.get():null;
	}
	
	public static <T extends ItemName>  T get(Collection<T> items, final Collection<String> guids) {
		Optional<T> anser = items.stream().filter( i -> guids.contains(i.getGUID())).findFirst();
		return anser.isPresent()?anser.get():null;
	}
	
	public static <T extends ItemName> boolean remove(Collection<T> items, final Collection<T> names, NameSegments segs) {
		boolean retval = false;
		Iterator<T> iter = WrappedIterator.create(items.iterator()).filterKeep(new ItemNameFilter<T>(names, segs));
		while (iter.hasNext())
		{
			iter.remove();
			retval = true;
		}
		return retval;
	}
	
	
	public static <T extends ItemName> int indexOf(Collection<T> items, final T name, NameSegments segs) {
		
		final ItemNameFilter<T> nof = new ItemNameFilter<T>(name,segs);
		final Iterator<T> iter = items.iterator();
		int i = 0;
		while (iter.hasNext()) {
			if (nof.test(iter.next())) {
				return i;
			}
			i++;
		}
		return -1;
	}
	
	private static class ItemNameFilter<T extends ItemName> implements Predicate<T> {

		private final Collection<T> others;
		private final NameSegments segs;
		

		public ItemNameFilter(final T other, NameSegments segs) {
			this.others = new ArrayList<T>();
			this.others.add(other);
			this.segs = segs;
		}

		public ItemNameFilter(final Collection<T> others, NameSegments segs) {
			this.others = others;
			this.segs = segs;
		}

		@Override
		public boolean test(final T name) {
			for (final T other : others) {
				if (other.matches(name,segs)) {
					return true;
				}
			}
			return false;
		}

	}

}
