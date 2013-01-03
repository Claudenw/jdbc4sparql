package org.xenei.jdbc4sparql.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;

public class CatalogImpl extends NamespaceImpl implements Catalog
{

	private final Map<String, Schema> schemas;

	public CatalogImpl( final String namespace, final String localName )
	{
		super(namespace, localName);
		this.schemas = new HashMap<String, Schema>();
	}

	public void addSchema( final Schema schema )
	{
		schemas.put(schema.getLocalName(), schema);
	}

	@Override
	public NameFilter<Schema> findSchemas( final String schemaNamePattern )
	{
		return new NameFilter<Schema>(schemaNamePattern, getSchemas());
	}

	@Override
	public Schema getSchema( final String schema )
	{
		return schemas.get(schema);
	}

	@Override
	public Set<Schema> getSchemas()
	{
		return new HashSet<Schema>(schemas.values());
	}

	@Override
	public String toString()
	{
		return String.format("Catalog[%s]", getLocalName());
	}

}
