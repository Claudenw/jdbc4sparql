package org.xenei.jdbc4sparql.impl.virtual;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.QExecutor;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.name.CatalogName;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;

/**
 * The Virtual catalog for all virtual schema and tables.
 *
 */
public class VirtualCatalog implements Catalog {
	public static final String NAME = "";
	private static final CatalogName name = new CatalogName(NAME);
	private Map<String, Schema> schemas;

	/**
	 * @return The default virtual catalog name.
	 */
	public static CatalogName getDefaultName()
	{
		return name;
	}
	
	public VirtualCatalog() {
		schemas = new HashMap<String, Schema>();
		schemas.put(VirtualSchema.NAME, new VirtualSchema(this));
	}

	@Override
	public void close() {
		schemas = null;
	}

	@Override
	public QExecutor getLocalExecutor() {
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

	@Override
	public String getShortName() {
		return getName().getCatalog();
	}
}
