package org.xenei.jdbc4sparql.iface;

import org.xenei.jdbc4sparql.impl.NameUtils;

/**
 * Name implementation.
 */
public class CatalogName extends ItemName
{
	private static String getCatalogName( ItemName name )
	{
		if (name instanceof CatalogName)
		{
			return ((CatalogName)name).getCatalog();
		}
		return null;
	}
	
	public CatalogName( final ItemName name )
	{
		super(getCatalogName(name), null, null);
	}

	public CatalogName( final String catalog )
	{
		super(catalog, null, null);
	}
	
	public String getCatalog()
	{
		return super.getSchema();
	}
	
	public String getSchema()
	{
		return null;
	}

	public String getShortName()
	{
		return getCatalog();
	}

	@Override
	public String toString()
	{
		return getShortName();
	}
}