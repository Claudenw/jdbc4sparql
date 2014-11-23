package org.xenei.jdbc4sparql.sparql.items;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import com.hp.hpl.jena.util.iterator.Filter;

public class NamedObjectGUIDFilter<T extends GUIDObject> extends Filter<T> {

	private Set<String> others;

	public NamedObjectGUIDFilter(String other) {
		this.others = new HashSet<String>();
		this.others.add(other);
	}

	public NamedObjectGUIDFilter(GUIDObject other) {
		this.others = new HashSet<String>();
		this.others.add(other.getGUID());
	}

	public NamedObjectGUIDFilter(Collection<GUIDObject> others) {
		this.others = new HashSet<String>();
		for (GUIDObject obj : others) {
			this.others.add(obj.getGUID());
		}
	}

	@Override
	public boolean accept(T item) {
		return others.contains(item.getGUID());
	}

}
