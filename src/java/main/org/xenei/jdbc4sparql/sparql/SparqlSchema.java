package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.query.QuerySolution;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.AbstractTable;
import org.xenei.jdbc4sparql.impl.DataTable;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;
import org.xenei.jdbc4sparql.impl.SchemaImpl;



public class SparqlSchema extends SchemaImpl
{
	

	//private SchemaBuilder builder;

	public SparqlSchema( SparqlCatalog catalog, String namespace, String localName, SchemaBuilder builder )
	{
		super( catalog, namespace, localName );
		addTableDefs( builder.getTableDefs());
	}
	
	public SparqlSchema( SparqlCatalog catalog, String namespace, String localName )
	{
		super( catalog, namespace, localName );
	}

	private SparqlTableDef verifySparqlTableDef( TableDef tableDef )
	{
		if (tableDef == null)
		{
			throw new IllegalArgumentException( "table def may not be a null");
		}
		if (! (tableDef instanceof SparqlTableDef))
		{
			throw new IllegalStateException( tableDef.getName()+" is not a SPARQL table definition.");
		}
		return (SparqlTableDef)tableDef;
	}
	
	/** Returns a table with no data
	 * 
	 * @param name
	 * @return
	 */
	public Table newTable( String name )
	{
		return new SparqlTable( this, verifySparqlTableDef(getTableDef(name)) );
	}

	@Override
	public void addTableDef( TableDef tableDef )
	{
		super.addTableDef(verifySparqlTableDef( tableDef ));
	}
}
