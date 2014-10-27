package org.xenei.jdbc4sparql.iface;

import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * Name implementation.
 */
public class TableName extends ItemName
{
	public static TableName getNameInstance( final String alias )
	{
		if (alias == null)
		{
			throw new IllegalArgumentException("Alias must be provided");
		}
		final String[] parts = alias.split("\\" + NameUtils.DB_DOT);
		switch (parts.length)
		{
			case 2:
				return new TableName(parts[0], parts[1]);
			case 1:
				return new TableName(null, parts[0]);

			default:
				throw new IllegalArgumentException(String.format(
						"Column name must be 1 or 2 segments not %s as in %s",
						parts.length, alias));

		}
	}

	public TableName( final ItemName name )
	{
		super(name.getSchema(), name.getTable(), null);
	}

	public TableName( final String schema, final String table )
	{
		super(schema, table, null);
	}

	public ColumnName getColumnName( final String colName )
	{
		return new ColumnName(getSchema(), getTable(), colName);
	}

	public TableName withSegments( final UsedSegments segments )
	{
		return new TableName(segments.getSchema(this), segments.getTable(this));
	}
}