package org.xenei.jdbc4sparql.sparql.items;

import java.util.ArrayList;
import java.util.Collection;

import com.hp.hpl.jena.util.iterator.Filter;

import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;

public class NamedObjectFilter<T extends NamedObject<?>> extends Filter<T> {

	protected Collection<ItemName> others;

	public NamedObjectFilter(ItemName other) {
		this.others = new ArrayList<ItemName>();
		this.others.add(other);
	}

	public NamedObjectFilter(Collection<?> others) {
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
	public boolean accept(T item) {
		ItemName name = item.getName().clone(NameSegments.ALL);
		for (ItemName other : others) {
			if (other.matches(name)) {
				return true;
			}
		}
		return false;
	}

}
