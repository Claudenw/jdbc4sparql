package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;

import org.xenei.jdbc4sparql.iface.ItemName;

/**
 * The base class for the Query items.
 *
 * @param <T>
 *            The type of the name for query item.
 */
public abstract class QueryItemInfo<T extends ItemName>
{
	private final T name;
	private final Var var;
	private final Var alias;
	private Expr expr;
	private boolean optional;

	protected QueryItemInfo( final T name, final boolean optional )
	{
		if (name == null)
		{
			throw new IllegalArgumentException("name may not be null");
		}
		this.name = name;
		this.var = Var.alloc(name.getSPARQLName());
		this.alias = Var.alloc(name.getAliasName());
		this.optional = optional;
		this.expr = null;
	}

	public Expr getExpr()
	{
		return expr;
	}

	/**
	 * Get the name for the query item.
	 *
	 * @return the item name.
	 */
	public T getName()
	{
		return name;
	}

	/**
	 * Get the variable for the query item.
	 *
	 * @return The variable based on the column name.
	 */
	public Var getVar()
	{
		return var;
	}
	
	/**
	 * Get the alias variable for this column
	 * @return The variable based on the alias from the columnName
	 */
	public Var getAlias()
	{
		return alias;
	}

	/**
	 * returns the optional flag.
	 *
	 * @return True if this item is optional.
	 */
	public boolean isOptional()
	{
		return optional;
	}

	public void setExpr( final Expr expr )
	{
		this.expr = expr;
	}

	/**
	 * Set the optional flag
	 *
	 * @param optional
	 *            The value of the optional flag.
	 */
	protected void setOptional( final boolean optional )
	{
		this.optional = optional;
	}

}
