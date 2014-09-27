package org.xenei.jdbc4sparql.sparql;

import org.xenei.jdbc4sparql.iface.ItemName;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

/**
 * The base class for the Query items.
 *
 * @param <T> The type of the name for query item.
 */
public abstract class QueryItemInfo<T extends ItemName> 
{
	private final T name;
	private final Var var;
	private Expr expr;
	private boolean optional;

	protected QueryItemInfo( final T name, boolean optional )
	{
		if (name == null)
		{
			throw new IllegalArgumentException( "name may not be null");
		}
		this.name = name;
		this.var = Var.alloc(name.getSPARQLName());
		this.optional = optional;
		this.expr = null;
	}
	
	public Expr getExpr()
	{
		return expr;
	}
	
	public void setExpr( Expr expr )
	{
		this.expr=expr;
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
	public Var getVar()
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
