package org.xenei.jdbc4sparql.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;

public class MockCatalog extends NamespaceImpl implements Catalog {

	public static final String NS = "http://examplec.com/namespace#";
	public static final String LOCAL_NAME="MockCatalog";
	List<Schema> schemas = new ArrayList<Schema>();
	
	public MockCatalog()
	{
		super( NS, LOCAL_NAME);
		schemas.add( new MockSchema( this ));
	}

	public void addSchema(Schema schema)
	{
		schemas.add( schema );
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
	
	@Override
	public NameFilter<Schema> findSchemas( String schemaNamePattern )
	{
		return new NameFilter<Schema>( schemaNamePattern, getSchemas());
	}
}