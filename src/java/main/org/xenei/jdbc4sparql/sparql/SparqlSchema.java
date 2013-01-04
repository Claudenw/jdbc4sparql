package org.xenei.jdbc4sparql.sparql;

import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.SchemaImpl;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;

public class SparqlSchema extends SchemaImpl
{

	public static final String DEFAULT_NAMESPACE="http://jdbc4sparql.xenei.org/namespace/schema#";


	public SparqlSchema( final SparqlCatalog catalog, final String namespace,
			final String localName )
	{
		super(catalog, namespace, localName);
	}

	@Override
	public void addTableDef( final TableDef tableDef )
	{
		super.addTableDef(verifySparqlTableDef(tableDef));
	}

	/**
	 * Returns a table with no data
	 * 
	 * @param name
	 * @return
	 */
	@Override
	public Table newTable( final String name )
	{
		return new SparqlTable(this, verifySparqlTableDef(getTableDef(name)));
	}

	private SparqlTableDef verifySparqlTableDef( final TableDef tableDef )
	{
		if (tableDef == null)
		{
			throw new IllegalArgumentException("table def may not be a null");
		}
		if (!(tableDef instanceof SparqlTableDef))
		{
			throw new IllegalStateException(tableDef.getLocalName()
					+ " is not a SPARQL table definition.");
		}
		return (SparqlTableDef) tableDef;
	}
}
