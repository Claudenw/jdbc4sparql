package org.xenei.jdbc4sparql.sparql.items;

import java.util.ArrayList;
import java.util.Collection;

import com.hp.hpl.jena.util.iterator.Filter;

import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;

public class NamedObjectFilter<T extends NamedObject<?>> extends Filter<T> {

	protected Collection<NamedObject<?>> others;

	public NamedObjectFilter(NamedObject<?> other) {
		this.others = new ArrayList<NamedObject<?>>();
		this.others.add(other);
	}

	public NamedObjectFilter(Collection<?> others) {
		this.others = new ArrayList<NamedObject<?>>();
		for (Object t : others) {
		   if (t instanceof NamedObject) {
				this.others.add((NamedObject<?>) t);
			} else {
				throw new IllegalArgumentException(String.format(
						"%s is not an instance of ItemName or NamedObject",
						t.getClass()));
			}
		}
	}

	@Override
	public boolean accept(T item) {
		for (NamedObject<?> other : others) {
			if (other.equals(item)) {
				return true;
			}
		}
		return false;
	}

}
