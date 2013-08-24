package org.xenei.jdbc4sparql.sparql;

import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;

/**
 * A column in a table in the query.  This class maps the column to an alias name.
 */
public class QueryColumnInfo extends QueryItemInfo<QueryColumnInfo.Name>
{
	/**
	 * A wild card column name.
	 */
	public static Name WILDNAME = new Name(null, null, null);
	
	/**
	 * The name for the QueryColumnInfo.
	 */
	public static class Name extends QueryItemName
	{
		
		private Name( final QueryItemName name )
		{
			super(name);
		}

		private Name( final String schema, final String table, final String col )
		{
			super(schema, table, col);
		}
	}

	public static Name getNameInstance( final QueryItemName name )
	{
		return new Name(name);
	}

	public static Name getNameInstance( final String alias )
	{
		if (alias == null)
		{
			throw new IllegalArgumentException("Alias must be provided");
		}
		final String[] parts = alias.split("\\" + NameUtils.DB_DOT);
		switch (parts.length)
		{
			case 3:
				return new Name(parts[0], parts[1], parts[2]);

			case 2:
				return new Name(null, parts[0], parts[1]);

			case 1:
				return new Name(null, null, parts[0]);

			default:
				throw new IllegalArgumentException(String.format(
						"Column name must be 1 to 3 segments not %s as in %s",
						parts.length, alias));
		}
	}

	public static Name getNameInstance( final String schemaName,
			final String tableName, final String columnName )
	{
		return new Name(schemaName, tableName, columnName);
	}

	private final RdfColumn column;

	public QueryColumnInfo( final QueryInfoSet infoSet, final QueryTableInfo tableInfo, final RdfColumn column,
			final QueryColumnInfo.Name alias, boolean optional)
	{
		super(alias, optional);
		if (column == null)
		{
			throw new IllegalArgumentException( "Column may not be null");
		}
		if (infoSet == null)
		{
			throw new IllegalArgumentException( "QueryTableInfo may not be null");
		}
		this.column = column;
		infoSet.addColumn(tableInfo, this);
	}

	public void setOptional( boolean optional )
	{
		super.setOptional( optional );
	}
	public RdfColumn getColumn()
	{
		return column;
	}

	@Override
	public String toString()
	{
		return String.format("QueryColumnInfo[%s(%s)]", column.getSQLName(),
				getName());
	}
	
	public boolean equals( Object o )
	{
		if (o != null && o instanceof QueryColumnInfo)
		{
			QueryColumnInfo colInfo = (QueryColumnInfo) o;
			return getName().equals( colInfo.getName() ) && column.equals(colInfo.getColumn());
		}
		return false;
	}
	
	public int hashCode() {
		return getName().hashCode();
	}
}
