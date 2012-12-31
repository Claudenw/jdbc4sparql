package org.xenei.jdbc4sparql.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.sparql.SparqlNamespace;

public class MockCatalog extends SparqlNamespace implements Catalog {

	List<Schema> schemas = new ArrayList<Schema>();
	
	public MockCatalog()
	{
		super( "http://examplec.com/namespace#", "MockCatalog");
		schemas.add( new MockSchema( this ));
	}

	@Override
	public Set<Schema> getSchemas()
	{
		return new HashSet<Schema>( schemas );
	}

	@Override
	public Schema getSchema( String schemaName )
	{
		Iterator<Schema> iter = new NameFilter<Schema>( schemaName, getSchemas() );
		if (iter.hasNext())
		{
			return iter.next();
		}
		else
		{
			Schema schema = new MockSchema( this, schemaName );
			schemas.add( schema );
			return schema;
		}
	}
	
}