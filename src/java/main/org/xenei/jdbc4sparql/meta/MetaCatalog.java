package org.xenei.jdbc4sparql.meta;

import org.xenei.jdbc4sparql.impl.CatalogImpl;

public class MetaCatalog extends CatalogImpl
{

	public static final String LOCAL_NAME = "Catalog";

	public MetaCatalog()
	{
		super(MetaNamespace.NS, MetaCatalog.LOCAL_NAME);
		addSchema(new MetaSchema(this));
	}
}
