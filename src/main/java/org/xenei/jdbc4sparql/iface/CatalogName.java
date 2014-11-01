package org.xenei.jdbc4sparql.iface;

import org.apache.commons.lang.StringUtils;

/**
 * Name implementation.
 */
public class CatalogName extends ItemName
{
	private static String getCatalogName( final ItemName name )
	{
		if (name instanceof CatalogName)
		{
			return ((CatalogName) name).getCatalog();
		}
		return null;
	}

	public CatalogName( final ItemName name )
	{
		super(CatalogName.getCatalogName(name), null, null);
	}

	public CatalogName( final String catalog )
	{
		super(ItemName.verifyOK("Catalog", catalog), null, null);
	}

	@Override
	protected String createName( final String separator )
	{
		return StringUtils.defaultString(getCatalog());
	}

	public String getCatalog()
	{
		return super.getSchema();
	}

	@Override
	public String getSchema()
	{
		return null;
	}

	@Override
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