package org.xenei.jdbc4sparql.impl.virtual;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QuerySolution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.name.CatalogName;

public class VirtualCatalog implements Catalog {
	public static final String NAME = "";
	private final CatalogName name = new CatalogName(NAME);
	private Map<String, Schema> schemas;

	public VirtualCatalog() {
		schemas = new HashMap<String, Schema>();
		schemas.put("", new VirtualSchema(this));
	}

	@Override
	public void close() {
		schemas = null;
	}

	@Override
	public List<QuerySolution> executeLocalQuery(final Query query) {
		return null;
	}

	@Override
	public NameFilter<Schema> findSchemas(final String schemaNamePattern) {
		return new NameFilter<Schema>(schemaNamePattern, schemas.values());
	}

	@Override
	public CatalogName getName() {
		return name;
	}

	@Override
	public Schema getSchema(final String schemaName) {
		return schemas.get(schemaName);
	}

	@Override
	public Set<Schema> getSchemas() {
		return new HashSet<Schema>(schemas.values());
	}

}
