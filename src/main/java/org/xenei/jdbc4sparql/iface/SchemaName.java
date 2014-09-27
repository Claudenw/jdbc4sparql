package org.xenei.jdbc4sparql.iface;

import org.xenei.jdbc4sparql.impl.NameUtils;

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
	
	public TableName getTableName( String tblName )
	{
		return new TableName( getSchema(), tblName );
	}

}