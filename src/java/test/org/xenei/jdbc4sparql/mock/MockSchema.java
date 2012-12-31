package org.xenei.jdbc4sparql.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.sparql.SparqlNamespace;

public class MockSchema extends SparqlNamespace implements Schema
{
	private Catalog catalog;
	private Map<String,Table> tables;
	
	public MockSchema()
	{
		this( new MockCatalog() );
	}
	
	public MockSchema(Catalog catalog)
	{
		this(catalog, "mockSchema");
	}
	
	public MockSchema(Catalog catalog, String schema)
	{
		super(catalog.getNamespace(), schema );
		this.catalog = catalog;
		this.tables = new HashMap<String,Table>();
	}

	@Override
	public Set<Table> getTables()
	{
		return Collections.emptySet();
	}

	@Override
	public Catalog getCatalog()
	{
		return catalog;
	}
	

	@Override
	public Table getTable( String tableName )
	{
		Table t = tables.get(tableName);
		if (t == null)
		{
			t = new MockTable( this, tableName );
			tables.put( tableName, t );
		}
		return  t;
	}
	
}