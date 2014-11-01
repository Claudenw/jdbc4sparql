package org.xenei.jdbc4sparql.iface;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.xenei.jdbc4sparql.impl.NameUtils;

public abstract class ItemName
{
	public static class UsedSegments
	{
		private boolean schema;
		private boolean table;
		private boolean column;

		public UsedSegments()
		{
			schema = true;
			table = true;
			column = true;
		}

		public UsedSegments( final boolean schema, final boolean table,
				final boolean column )
		{
			this.schema = schema;
			this.table = table;
			this.column = column;
		}

		public String getColumn( final ItemName name )
		{
			return column ? name.getCol() : null;
		}

		public String getSchema( final ItemName name )
		{
			return schema ? name.getSchema() : null;
		}

		public String getTable( final ItemName name )
		{
			return table ? name.getTable() : null;
		}

		public void setColumn( final boolean column )
		{
			this.column = column;
		}

		public void setSchema( final boolean schema )
		{
			this.schema = schema;
		}

		public void setTable( final boolean table )
		{
			this.table = table;
		}
		
		@Override
		public String toString()
		{
			return String.format( "S:%s T:%s C:%s", schema, table, column);
		}

	}

	protected static String verifyOK( final String segment, final String value )
	{
		if (value != null)
		{
			for (final String badChar : NameUtils.DOT_LIST)
			{
				if (value.contains(badChar))
				{
					throw new IllegalArgumentException(String.format(
							"%s name may not contain '%s'", segment, badChar));
				}
			}
		}
		return value;
	}

	static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";
	private final String schema;

	private final String table;

	private final String col;

	protected ItemName( final ItemName name )
	{
		this.schema = name.getSchema();
		this.table = name.getTable();
		this.col = name.getCol();
	}

	protected ItemName( final String schema, final String table,
			final String col )
	{
		this.schema = verifyOK("Schema", schema);
		this.table = verifyOK("Table", table );
		this.col = verifyOK("Column", col );
	}

	/**
	 * create the fully qualified name for this item.
	 * @param separator The string to use between name segments
	 * @return The fully qualified name
	 */
	abstract protected String createName( final String separator );

	@Override
	public boolean equals( final Object o )
	{
		if (super.equals(o))
		{
			return true;
		}
		if (this.getClass().equals(o.getClass()))
		{
			final ItemName n = (ItemName) o;
			return new EqualsBuilder().append(this.getClass(), o.getClass())
					.append(this.getSchema(), n.getSchema())
					.append(this.getTable(), n.getTable())
					.append(this.getCol(), n.getCol()).isEquals();

		}
		return false;
	}

	/**
	 * Find the object matching the key in the map.
	 * Uses matches() method to determine match.
	 *
	 * @param map
	 *            The map to find the object in.
	 * @return The Object (T) or null if not found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public <T> T findMatch( final Map<? extends ItemName, T> map )
	{
		// exact match
		if (map.containsKey(this))
		{
			return map.get(this);
		}
		T retval = null;
		// no match pattern
		if (isWild())
		{
			if (map.size() > 0)
			{
				// no match pattern so see if there is only one table in the
				// list
				retval = map.values().iterator().next();
				if (map.size() == 1)
				{
					return retval;
				}
				throw new IllegalArgumentException(String.format(
						ItemName.FOUND_IN_MULTIPLE_, this, retval.getClass()
								.getSimpleName()));
			}
		}
		else if (hasWild())
		{
			for (final ItemName n : map.keySet())
			{
				if (matches(n))
				{
					if (retval != null)
					{
						throw new IllegalArgumentException(String.format(
								ItemName.FOUND_IN_MULTIPLE_, this, retval
										.getClass().getSimpleName()));
					}
					retval = map.get(n);
				}
			}
		}
		return retval;
	}

	/**
	 * Get the column name string
	 *
	 * @return
	 */
	public String getCol()
	{
		return col;
	}

	/**
	 * Get the name in DB format
	 *
	 * @return
	 */
	public String getDBName()
	{
		return createName(NameUtils.DB_DOT);
	}

	/**
	 * Get the schema segment of the name.
	 *
	 * @return
	 */
	public String getSchema()
	{
		return schema;
	}

	abstract public String getShortName();

	/**
	 * Get the complete name in SPARQL format
	 *
	 * @return
	 */
	public String getSPARQLName()
	{
		return createName(NameUtils.SPARQL_DOT);
	}
	
	/**
	 * Get the name as a UUID based on the real name.
	 * @return the UUID based name
	 */
	public String getAliasName()
	{
		return "v_"+(UUID.nameUUIDFromBytes(getSPARQLName().getBytes()).toString().replace("-", "_"));
	}

	/**
	 * Get the table portion of the complete name.
	 *
	 * @return
	 */
	public String getTable()
	{
		return table;
	}

	@Override
	public int hashCode()
	{
		return new HashCodeBuilder().append(getSchema()).append(getTable())
				.append(getCol()).toHashCode();
	}

	/**
	 * @return true if this has a wildcard (null) segment
	 */
	public boolean hasWild()
	{
		return (getSchema() == null) || (getTable() == null)
				|| (getCol() == null);
	}

	/**
	 * @return true if this is a complete wildcard name (e.g. all segments are
	 *         null)
	 */
	public boolean isWild()
	{
		return (getSchema() == null) && (getTable() == null)
				&& (getCol() == null);
	}

	/**
	 * Find the object matching the key in the map.
	 * Uses matches() method to determine match.
	 *
	 * @param map
	 *            The map to find the object in.
	 * @return The Object (T) or null if not found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public <T> Set<T> listMatches( final Map<? extends ItemName, T> map )
	{
		final Set<T> retval = new HashSet<T>();
		for (final ItemName n : map.keySet())
		{
			if (matches(n))
			{
				retval.add(map.get(n));
			}
		}
		return retval;
	}

	/**
	 * check if this matches that.
	 *
	 * this matches that if this.equals( that ) or
	 * if this.schema, this.table, this.col == null then that
	 * segment is not used in the comparison.
	 *
	 * Note that this is a.matches(b) does not imply b.matches(a)
	 *
	 * @param n
	 * @return
	 */
	public boolean matches( final ItemName that )
	{
		if (that == null)
		{
			return false;
		}
		final EqualsBuilder eb = new EqualsBuilder().append(this.getClass(),
				that.getClass());
		if (this.getSchema() != null)
		{
			eb.append(this.getSchema(), that.getSchema());
		}
		if (this.getTable() != null)
		{
			eb.append(this.getTable(), that.getTable());
		}
		if (this.getCol() != null)
		{
			eb.append(this.getCol(), that.getCol());
		}
		return eb.isEquals();
	}

	@Override
	public String toString()
	{
		if (isWild())
		{
			return "Wildcard Name";
		}
		return StringUtils.defaultIfBlank(getDBName(), "Blank Name");
	}
}