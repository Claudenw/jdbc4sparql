package org.xenei.jdbc4sparql.mock;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.impl.SchemaImpl;

public class MockSchema extends SchemaImpl
{
	public static final String LOCAL_NAME = "MockSchema";

	public MockSchema()
	{
		this(new MockCatalog());
	}

	public MockSchema( final Catalog catalog )
	{
		this(catalog, MockSchema.LOCAL_NAME);
	}

	public MockSchema( final Catalog catalog, final String schema )
	{
		super(catalog, MockCatalog.NS, schema);
	}

}