package org.xenei.jdbc4sparql.sparql.builders;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.ColumnDefImpl;
import org.xenei.jdbc4sparql.sparql.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlColumn;
import org.xenei.jdbc4sparql.sparql.SparqlTableDef;

public class SimpleBuilder implements SchemaBuilder
{

	// Params: namespace.
	private static final String TABLE_QUERY="prefix afn: <http://jena.hpl.hp.com/ARQ/function#>. " +
			"SELECT ?tName WHERE { ?tName a rdfs:class ; " +
			"FILTER( afn:namespace(?tName) == '%s') }";
	
	// Params: class resource, namespace
	private static final String COLUMN_QUERY="prefix afn: <http://jena.hpl.hp.com/ARQ/function#> . " +
			"SELECT DISTINCT ?cName " +
			"WHERE { " +
			"[] rdfs:domain <%s> ; " +
			" ?cname [] ;" +
			" FILTER( afn:namespace(?cName) == '%s') }";	
	
	private SparqlCatalog catalog;
	private String namespace;
	
	public SimpleBuilder(SparqlCatalog catalog, String namespace)
	{
		this.catalog = catalog;
	}

	@Override
	public Set<TableDef> getTableDefs()
	{
		HashSet<TableDef> retval = new HashSet<TableDef>();
		List<QuerySolution> solns = catalog.executeQuery( String.format( TABLE_QUERY, namespace ));
		for (QuerySolution soln : solns )
		{
			Resource tName = soln.getResource("tName");
			SparqlTableDef tableDef = new SparqlTableDef(  tName.getLocalName(), null );
			addColumnDefs( tableDef, tName );
			
			retval.add( tableDef); 
		}
		return retval;
	}
	
	private void addColumnDefs( SparqlTableDef tableDef, Resource tName )
	{
		List<QuerySolution> solns = catalog.executeQuery( String.format( COLUMN_QUERY, tName, namespace ));
		for (QuerySolution soln : solns )
		{
			Resource cName = soln.getResource("cName");
			tableDef.add( ColumnDefImpl.getStringInstance(namespace, cName.getLocalName()));
		}
		
	}

}
