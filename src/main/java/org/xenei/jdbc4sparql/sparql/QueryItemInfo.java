package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

/**
 * The base class for the Query items.
 *
 * @param <T> The type of the name for query item.
 */
public abstract class QueryItemInfo<T extends QueryItemName>
{
	private final T name;
	private final Node var;
	private boolean optional;

	protected QueryItemInfo( final T name, boolean optional )
	{
		if (name == null)
		{
			throw new IllegalArgumentException( "name may not be null");
		}
		this.name = name;
		this.var = NodeFactory.createVariable(name.getSPARQLName());
		this.optional = optional;
	}

	/**
	 * Get the name for the query item.
	 * @return the item name.
	 */
	public T getName()
	{
		return name;
	}

	/**
	 * Get the variable for the query item.
	 * @return The variable node
	 */
	public Node getVar()
	{
		return var;
	}
	/**
	 * returns the optional flag.
	 * @return True if this item is optional.
	 */
	public boolean isOptional()
	{
		return optional;
	}
	
	/**
	 * Set the optional flag
	 * @param optional The value of the optional flag.
	 */
	protected void setOptional( boolean optional )
	{
		this.optional = optional;
	}

}
