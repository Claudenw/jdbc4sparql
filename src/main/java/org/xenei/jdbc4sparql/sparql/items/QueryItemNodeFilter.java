package org.xenei.jdbc4sparql.sparql.items;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.xenei.jdbc4sparql.iface.name.ItemName;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.iterator.Filter;

public class QueryItemNodeFilter<T extends QueryItemInfo<?>> extends Filter<T> {

	List<Node> others;

	public QueryItemNodeFilter(Node other) {
		this.others = new ArrayList<Node>();
		others.add(other);
	}

	public QueryItemNodeFilter(Collection<QueryItemInfo<?>> others) {
		this.others = new ArrayList<Node>();
		for (QueryItemInfo<?> qii : others) {
			this.others.add(qii.getVar().asNode());
		}
	}

	@Override
	public boolean accept(T item) {
		ItemName name = item.getName(); // .clone( NameSegments.ALL );
		return others.contains(Var.alloc(name.getSPARQLName()).asNode());
	}

}
