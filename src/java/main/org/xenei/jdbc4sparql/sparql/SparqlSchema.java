package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.query.QuerySolution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;



public class SparqlSchema extends NamespaceImpl implements Schema
{
	private static final String tableNames="prefix afn: <http://jena.hpl.hp.com/ARQ/function#>. " +
			"SELECT ?tName WHERE { ?tName a rdfs:class ; " +
			"FILTER( afn:namespace(?Name) == '%s') }";

	private SparqlCatalog catalog;
	private Map<String,SparqlTableDef> tables;

	public SparqlSchema( SparqlCatalog catalog, String namespace, String localName )
	{
		super( namespace, localName );
		this.catalog=catalog;
		tables = new HashMap<String,SparqlTableDef>();
		List<QuerySolution> solns = catalog.executeQuery( String.format( tableNames, namespace ));
		for (QuerySolution soln : solns )
		{
			SparqlTableDef tableDef = new SparqlTableDef( this, soln.getResource("tName") ); 
			tables.put( tableDef.getName(), tableDef); 
		}
		
	}
	
	@Override
	public Set<Table> getTables()
	{
		
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SparqlCatalog getCatalog()
	{
		return catalog;
	}

	@Override
	public Table getTable( String tableName )
	{
		SparqlTableDef tblDef = tables.get( tableName );
		if (tblDef == null)
		{
			throw new IllegalArgumentException( String.format("Table %s not found in schema %s", tableName, getLocalName()));
		}
		return new SparqlTable( this, tblDef );
	}
	
	@Override
	public NameFilter<Table> findTables( String tableNamePattern )
	{
		return new NameFilter<Table>( tableNamePattern, getTables());
	}

}
