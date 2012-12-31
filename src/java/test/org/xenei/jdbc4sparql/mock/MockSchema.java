package org.xenei.jdbc4sparql.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.SchemaImpl;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;

public class MockSchema extends SchemaImpl
{
	public static final String LOCAL_NAME="MockSchema";
	
	public MockSchema()
	{
		this( new MockCatalog() );
	}
	
	public MockSchema(Catalog catalog)
	{
		this(catalog, LOCAL_NAME);
	}
	
	public MockSchema(Catalog catalog, String schema)
	{
		super(catalog, MockCatalog.NS, schema );
	}


}