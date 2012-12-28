package org.xenei.jdbc4sparql.mock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;

public class MockCatalog implements Catalog {

	@Override
	public String getNamespace()
	{
		return "http://examplec.com/namespace#";

	}

	@Override
	public String getLocalName()
	{
		return "MockCatalog";
	}

	@Override
	public Set<Schema> getSchemas()
	{
		return new HashSet<Schema>( Arrays.asList( new Schema[] { new MockSchema() } ));
	}

	@Override
	public Schema getSchema( String schema )
	{
		Iterator<Schema> iter = new NameFilter<Schema>( schema, getSchemas() );
		return iter.hasNext()?iter.next():null;
	}
	
}