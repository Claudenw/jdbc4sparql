package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.net.URL;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;

public class SparqlCatalog extends NamespaceImpl implements Catalog
{

	private URL sparqlEndpoint; 
	
	public SparqlCatalog( URL sparqlEndpoint, String localName )
	{
		super( sparqlEndpoint.toString(), localName );
		this.sparqlEndpoint = sparqlEndpoint;
	}

	@Override
	public Set<Schema> getSchemas()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema( String schema )
	{
		// TODO Auto-generated method stub
		return null;
	}

	public List<QuerySolution> executeQuery( String queryStr )
	{
		QueryExecution qexec = QueryExecutionFactory.sparqlService( sparqlEndpoint.toString(), queryStr );
		try {
		    return WrappedIterator.create(qexec.execSelect()).toList();
		  } finally { qexec.close() ; }
	}


	@Override
	public NameFilter<Schema> findSchemas( String schemaNamePattern )
	{
		return new NameFilter<Schema>( schemaNamePattern, getSchemas());
	}
}
