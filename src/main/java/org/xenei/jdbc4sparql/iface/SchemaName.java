package org.xenei.jdbc4sparql.iface;


/**
 * Name implementation.
 */
public class SchemaName extends ItemName
{
	public SchemaName( final ItemName name )
	{
		super(name.getSchema(), null, null);
	}

	public SchemaName( final String schema )
	{
		super(schema, null, null);
	}

	public TableName getTableName( final String tblName )
	{
		return new TableName(getSchema(), tblName);
	}

	public SchemaName withSegments( final UsedSegments segments )
	{
		return new SchemaName(segments.getSchema(this));
	}

}