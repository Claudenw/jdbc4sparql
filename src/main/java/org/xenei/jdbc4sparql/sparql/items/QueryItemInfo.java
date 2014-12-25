package org.xenei.jdbc4sparql.sparql.items;

import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;

import com.hp.hpl.jena.sparql.core.Var;

/**
 * The base class for the Query items.
 *
 * @param <N>
 *            The type of the name for query item.
 */
public class QueryItemInfo<T extends NamedObject<N>, N extends ItemName>
implements NamedObject<N>, GUIDObject {
	private final T baseObject;
	private final N name;
	private final Var baseVar;
	private boolean optional;

	protected QueryItemInfo(final T baseObject, final N name,
			final boolean optional) {
		if (baseObject == null) {
			throw new IllegalArgumentException("baseObject may not be null");
		}
		if (name == null) {
			throw new IllegalArgumentException("name may not be null");
		}
		this.baseObject = baseObject;
		this.name = name;
		this.baseVar = Var.alloc(this.name.getGUID());
		this.optional = optional;

	}

	@Override
	public boolean equals(final Object o) {
		if (o instanceof QueryItemInfo) {
			final QueryItemInfo<?, ?> other = (QueryItemInfo<?, ?>) o;
			return this.getName().equals(other.getName());
			// && this.isOptional() == other.isOptional();
		}
		return false;
	}

	protected T getBaseObject() {
		return baseObject;
	}

	@Override
	public String getGUID() {
		return baseObject.getName().getGUID();
	}

	/**
	 * Get the alias variable for this column
	 *
	 * @return The variable based on the alias from the columnName
	 */
	public Var getGUIDVar() {
		return baseVar;
	}

	/**
	 * Get the name for the query item.
	 *
	 * @return the item name.
	 */
	@Override
	public N getName() {
		return name;
	}

	public NameSegments getSegments() {
		return this.name.getUsedSegments();
	}

	/**
	 * Get the variable for the query item.
	 *
	 * @return The variable based on the column name.
	 */
	public Var getVar() {
		return Var.alloc(this.name.getSPARQLName());
	}

	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	/**
	 * returns the optional flag.
	 *
	 * @return True if this item is optional.
	 */
	public boolean isOptional() {
		return optional;
	}

	/**
	 * Set the optional flag
	 *
	 * @param optional
	 *            The value of the optional flag.
	 */
	protected void setOptional(final boolean optional) {
		this.optional = optional;
	}

	protected void setSegments(final NameSegments usedSegments) {
		this.name.setUsedSegments(usedSegments);
	}

	@Override
	public String toString() {
		return getName().toString();
	}

}
