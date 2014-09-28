package org.xenei.jdbc4sparql.iface;

import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * The name for the QueryColumnInfo.
 */
public class ColumnName extends ItemName
{
	private TableName tableName = null;
	
	/**
	 * A wild card column name.
	 */
	public static ColumnName WILDNAME = new ColumnName(null, null, null);

	public ColumnName( final ItemName name )
	{
		super(name);
	}

	public ColumnName( final String schema, final String table, final String col )
	{
		super(schema, table, col);
	}
	
	public TableName getTableName()
	{
		if (tableName == null)
		{
			tableName = new TableName( this );
		}
		return tableName;
	}

	public static ColumnName getNameInstance( final String schemaName,
			final String tableName, final String columnName )
	{
		return new ColumnName(schemaName, tableName, columnName);
	}

	public static ColumnName getNameInstance( final String alias )
	{
		if (alias == null)
		{
			throw new IllegalArgumentException("Alias must be provided");
		}
		final String[] parts = alias.split("\\" + NameUtils.DB_DOT);
		switch (parts.length)
		{
			case 3:
				return new ColumnName(parts[0], parts[1], parts[2]);
	
			case 2:
				return new ColumnName(null, parts[0], parts[1]);
	
			case 1:
				return new ColumnName(null, null, parts[0]);
	
			default:
				throw new IllegalArgumentException(String.format(
						"Column name must be 1 to 3 segments not %s as in %s",
						parts.length, alias));
		}
	}

	public static ColumnName getNameInstance( final ItemName name )
	{
		return new ColumnName(name);
	}

}