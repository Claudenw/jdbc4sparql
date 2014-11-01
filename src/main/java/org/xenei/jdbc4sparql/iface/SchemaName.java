package org.xenei.jdbc4sparql.iface;

import org.apache.commons.lang.StringUtils;

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
	
	@Override
	protected String createName( final String separator )
	{
		return StringUtils.defaultString(getSchema());
	}
	
	@Override
	public String getShortName() {
		return getSchema();
	}

}