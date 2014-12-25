package org.xenei.jdbc4sparql.sparql.items;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.name.GUIDObject;

import com.hp.hpl.jena.util.iterator.Filter;

public class NamedObjectGUIDFilter<T extends GUIDObject> extends Filter<T> {

	private final Set<String> others;

	public NamedObjectGUIDFilter(final String other) {
		this.others = new HashSet<String>();
		this.others.add(other);
	}

	public NamedObjectGUIDFilter(final GUIDObject other) {
		this.others = new HashSet<String>();
		this.others.add(other.getGUID());
	}

	public NamedObjectGUIDFilter(final Collection<GUIDObject> others) {
		this.others = new HashSet<String>();
		for (final GUIDObject obj : others) {
			this.others.add(obj.getGUID());
		}
	}

	@Override
	public boolean accept(final T item) {
		return others.contains(item.getGUID());
	}

}
