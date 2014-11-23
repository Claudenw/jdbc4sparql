package org.xenei.jdbc4sparql.sparql.items;

//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.List;
//
//import com.hp.hpl.jena.sparql.core.Var;
//import com.hp.hpl.jena.util.iterator.Filter;
//
//public class QueryItemVarFilter<T extends QueryItemInfo<?>> extends Filter<T> {
//
//	List<Var> others;
//	
//	public QueryItemVarFilter(Var other) {
//		this.others = new ArrayList<Var>();
//		others.add(other);
//	}
//
//	public QueryItemVarFilter(Collection<QueryItemInfo<?>> others) {
//		this.others = new ArrayList<Var>();
//		for (QueryItemInfo<?> qii : others)
//		{
//			this.others.add(qii.getVar());
//		}
//	}
//
//	@Override
//	public boolean accept(T item) {
//		return others.contains( item.getVar() );
//	}
//
// }
