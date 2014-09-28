package org.xenei.jdbc4sparql.impl.virtual;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.CatalogName;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QuerySolution;

public class VirtualCatalog implements Catalog {
	private CatalogName name = new CatalogName( "" );
	private Map<String,Schema> schemas;

	public VirtualCatalog() {
		schemas = new HashMap<String,Schema>();
		schemas.put( "",  new VirtualSchema(this));
	}

	@Override
	public CatalogName getName() {
		return name;
	}

	@Override
	public void close() {
		schemas = null;
	}

	@Override
	public List<QuerySolution> executeLocalQuery(Query query) {
		return null;
	}

	@Override
	public NameFilter<Schema> findSchemas(String schemaNamePattern) {
		return new NameFilter<Schema>(schemaNamePattern, schemas.values());
	}

	@Override
	public Schema getSchema(String schemaName) {
		return schemas.get(schemaName);
	}

	@Override
	public Set<Schema> getSchemas() {
		return new HashSet<Schema>(schemas.values());
	}

}
