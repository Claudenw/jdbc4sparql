package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QueryItemInfo
{
	private final QueryItemName name;
	private final Node var;

	QueryItemInfo( final QueryItemName name )
	{
		this.name = name;
		this.var = NodeFactory.createVariable(name.getSPARQLName());
	}

	public QueryItemName getName()
	{
		return name;
	}

	public Node getVar()
	{
		return var;
	}

}
