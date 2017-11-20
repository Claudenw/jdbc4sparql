package org.xenei.jdbc4sparql.sparql.items;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.jena.util.iterator.Filter;

public class NamedObjectFilter<T extends NamedObject<?>> extends Filter<T> {

	protected Collection<NamedObject<?>> others;

	public NamedObjectFilter(final NamedObject<?> other) {
		this.others = new ArrayList<NamedObject<?>>();
		this.others.add(other);
	}

	public NamedObjectFilter(final Collection<?> others) {
		this.others = new ArrayList<NamedObject<?>>();
		for (final Object t : others) {
			if (t instanceof NamedObject) {
				this.others.add((NamedObject<?>) t);
			}
			else {
				throw new IllegalArgumentException(String.format(
						"%s is not an instance of ItemName or NamedObject",
						t.getClass()));
			}
		}
	}

	@Override
	public boolean accept(final T item) {
		for (final NamedObject<?> other : others) {
			if (other.equals(item)) {
				return true;
			}
		}
		return false;
	}

}
