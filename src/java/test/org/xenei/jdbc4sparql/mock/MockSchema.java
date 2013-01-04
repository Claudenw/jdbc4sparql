package org.xenei.jdbc4sparql.mock;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.impl.SchemaImpl;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlSchema;

public class MockSchema extends SparqlSchema
{
	public static final String LOCAL_NAME = "MockSchema";

	public MockSchema()
	{
		this(new MockCatalog());
	}

	public MockSchema( final SparqlCatalog catalog )
	{
		this(catalog, MockSchema.LOCAL_NAME);
	}

	public MockSchema( final SparqlCatalog catalog, final String schema )
	{
		super(catalog, MockCatalog.NS, schema);
	}

	public Table newTable( final String name )
	{
		final TableDef tableDef = getTableDef(name);
		if (tableDef == null)
		{
			throw new IllegalArgumentException(name
					+ " is not a table in this schema");
		}
		return new MockTable(this, (MockTableDef)tableDef);
	}
}