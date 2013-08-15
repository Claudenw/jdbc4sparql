package org.xenei.jdbc4sparql.sparql;

import org.apache.jena.atlas.logging.Log;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;

public class QueryColumnInfo extends QueryItemInfo
{
	private static class Name extends QueryItemName
	{

		private Name( QueryItemName name )
		{
			super( name );
		}
		
		private Name( final String schema, final String table, final String col )
		{
			super(schema, table, col);
		}
	}

	public static QueryItemName getNameInstance( final QueryItemName name)
	{
		return new Name( name );
	}
	
	public static QueryItemName getNameInstance( final String alias )
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

	public static QueryItemName getNameInstance( final String schemaName,
			final String tableName, final String columnName )
	{
		return new Name(schemaName, tableName, columnName);
	}

	private final RdfColumn column;
	private final QueryTableSet tableSet;

	public QueryColumnInfo( final QueryTableSet tableSet,
			final RdfColumn column, final String alias )
	{
		super(QueryColumnInfo.getNameInstance(alias));
		this.column = column;
		this.tableSet = tableSet;
		tableSet.addColumn(this);
	}
	
	public QueryColumnInfo( QueryColumnInfo info, QueryItemName name )
	{
		super(name);
		this.column = info.column;
		this.tableSet = info.tableSet;
	}
	
	public RdfColumn getColumn()
	{
		return column;
	}

	public boolean isOptional()
	{
		return column.isOptional();
	}

	@Override
	public String toString()
	{
		return String.format("QueryColumnInfo[%s(%s)]", column.getSQLName(),
				getName());
	}
}
