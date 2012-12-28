package org.xenei.jdbc4sparql.mock;

import java.util.Collections;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;

public class MockSchema implements Schema
{
	public MockSchema()
	{
		
	}
	@Override
	public String getNamespace()
	{
		return "http://examplec.com/namespace#";
	}

	@Override
	public String getLocalName()
	{
		return "mockSchema";
	}

	@Override
	public Set<Table> getTables()
	{
		return Collections.emptySet();
	}

	@Override
	public Catalog getCatalog()
	{
		return new MockCatalog();
	}
	
}