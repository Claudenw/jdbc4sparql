package org.xenei.jdbc4sparql.sparql.builders;

import com.hp.hpl.jena.query.QuerySolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.SparqlColumn;
import org.xenei.jdbc4sparql.sparql.SparqlTableDef;

public class SimpleBuilder implements SchemaBuilder
{

	private static final String tableNames="prefix afn: <http://jena.hpl.hp.com/ARQ/function#>. " +
			"SELECT ?tName WHERE { ?tName a rdfs:class ; " +
			"FILTER( afn:namespace(?Name) == '%s') }";
	
	private static final String columnInfo="prefix afn: <http://jena.hpl.hp.com/ARQ/function#> . " +
			"SELECT DISTINCT ?col " +
			"WHERE { " +
			"[] rdfs:domain <%s> ; " +
			"<?col> [] ;" +
			" FILTER( afn:namespace(?col) == '%s') }";	
	
	private Catalog catalog;
	public SimpleBuilder(Catalog catalog)
	{
		this.catalog = catalog;
	}

	@Override
	public Set<TableDef> getTableDefs( Catalog catalog )
	{
		List<QuerySolution> solns = catalog.executeQuery( String.format( tableNames, namespace ));
		for (QuerySolution soln : solns )
		{
			SparqlTableDef tableDef = new SparqlTableDef( this, soln.getResource("tName") ); 
			tables.put( tableDef.getName(), tableDef); 
		}
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String getName()
	{
		return resource.getLocalName();
	}

	
	private synchronized void createColumns()
	{
		if (columns != null)
		{
			return;		// just in case it was created while we waited.
		}
		
		columns = new ArrayList<ColumnDef>();
		List<QuerySolution> solns = schema.getCatalog().executeQuery( String.format( columnInfo, getName(), resource.getNameSpace() ));
		for (QuerySolution sol : solns)
		{
			columns.add( new SparqlColumn( this, sol.getResource( "col")));
		}
	}

}
