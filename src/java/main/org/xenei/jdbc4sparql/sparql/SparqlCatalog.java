package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.net.URL;
import java.util.List;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.impl.CatalogImpl;
import org.xenei.jdbc4sparql.impl.NamespaceImpl;

public class SparqlCatalog extends CatalogImpl
{

	private URL sparqlEndpoint; 
	private Model localModel;
	
	public SparqlCatalog( URL sparqlEndpoint, String localName )
	{
		super( sparqlEndpoint.toString(), localName );
		this.sparqlEndpoint = sparqlEndpoint;
	}
	
	public SparqlCatalog( String namespace, Model localModel, String localName )
	{
		super( namespace, localName );
		this.localModel = localModel;
	}

	public SparqlSchema getViewSchema()
	{
		return new SparqlSchema( this, SparqlView.NAME_SPACE, "" );
	}
	
	public List<QuerySolution> executeQuery( String queryStr )
	{
		return executeQuery( QueryFactory.create(queryStr));
	}
	
	public List<QuerySolution> executeQuery( Query query )
	{
		QueryExecution qexec = null;
		if (localModel == null)
		{
			qexec = QueryExecutionFactory.sparqlService( sparqlEndpoint.toString(), query );
		}
		else
		{
			qexec =  QueryExecutionFactory.create( query, localModel );
		}
		try {
		    return WrappedIterator.create(qexec.execSelect()).toList();
		  } finally { qexec.close() ; }
	}


}
