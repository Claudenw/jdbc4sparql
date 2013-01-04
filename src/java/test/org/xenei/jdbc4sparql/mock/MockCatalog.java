package org.xenei.jdbc4sparql.mock;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;

public class MockCatalog extends SparqlCatalog
{
	public static final URL MOCK_URL;
	public static final String NS = "http://examplec.com/namespace#";
	public static final String LOCAL_NAME = "MockCatalog";
	List<Schema> schemas = new ArrayList<Schema>();

	static {
		 try
		{
			MOCK_URL = new URL( "http://example.com/namespace");
		}
		catch (MalformedURLException e)
		{
			throw new RuntimeException( e );
		}
	}
	
	public MockCatalog()
	{
		super(MOCK_URL, MockCatalog.LOCAL_NAME);
		schemas.add(new MockSchema(this));
	}

	public void addSchema( final Schema schema )
	{
		schemas.add(schema);
	}

	@Override
	public NameFilter<Schema> findSchemas( final String schemaNamePattern )
	{
		return new NameFilter<Schema>(schemaNamePattern, getSchemas());
	}

	@Override
	public Schema getSchema( final String schemaName )
	{
		final Iterator<Schema> iter = new NameFilter<Schema>(schemaName,
				getSchemas());
		if (iter.hasNext())
		{
			return iter.next();
		}
		else
		{
			final Schema schema = new MockSchema(this, schemaName);
			schemas.add(schema);
			return schema;
		}
	}

	@Override
	public Set<Schema> getSchemas()
	{
		return new HashSet<Schema>(schemas);
	}
}