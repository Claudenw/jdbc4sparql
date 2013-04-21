/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.net.URL;
import java.util.List;

import org.xenei.jdbc4sparql.config.ModelReader;
import org.xenei.jdbc4sparql.impl.CatalogImpl;

/**
 * An implementation of sparql catalog.
 */
public class SparqlCatalog extends CatalogImpl
{
	// either the sparqlEndpoint or the localModel is set.

	// The URL for the sparql endpoint
	private URL sparqlEndpoint;
	// the model that contains the sparql data.
	private Model localModel;

	/**
	 * Constructor for a local model.
	 * 
	 * @param namespace
	 *            The namespace for the catalog.
	 * @param localModel
	 *            The model that contains the data.
	 * @param localName
	 *            The local name for the catalog.
	 */
	public SparqlCatalog( final String namespace, final Model localModel,
			final String localName )
	{
		super(namespace, localName);
		this.localModel = localModel;
	}

	/**
	 * Constructor for a remote sparql endpoint.
	 * 
	 * The namespace for the catalog will be the the sparqlEndpoint.
	 * 
	 * @param sparqlEndpoint
	 *            The sparql endpoint
	 * @param localName
	 *            The localname.
	 */
	public SparqlCatalog( final URL sparqlEndpoint, final String localName )
	{
		super(sparqlEndpoint.toString(), localName);
		this.sparqlEndpoint = sparqlEndpoint;
		// we need an empty model to properly filter sparql queries.
		this.localModel = ModelFactory.createMemModelMaker().createFreshModel();
	}

	/**
	 * Execute the query against the local Model.
	 * 
	 * This is used to execute queries built by the query builder.
	 * 
	 * @param query
	 * @return The list of QuerySolutions.
	 */
	public List<QuerySolution> executeLocalQuery( final Query query )
	{
		final QueryExecution qexec = QueryExecutionFactory.create(query,
				localModel);

		try
		{
			return WrappedIterator.create(qexec.execSelect()).toList();
		}
		finally
		{
			qexec.close();
		}
	}

	/**
	 * Execute a jena query against the data.
	 * 
	 * @param query
	 *            The query to execute.
	 * @return The list of QuerySolutions.
	 */
	public List<QuerySolution> executeQuery( final Query query )
	{
		QueryExecution qexec = null;
		if (isService())
		{
			qexec = QueryExecutionFactory.sparqlService(
					sparqlEndpoint.toString(), query);
		}
		else
		{
			qexec = QueryExecutionFactory.create(query, localModel);
		}
		try
		{
			return WrappedIterator.create(qexec.execSelect()).toList();
		}
		finally
		{
			qexec.close();
		}
	}

	/**
	 * Execute a query against the data.
	 * 
	 * @param queryStr
	 *            The query as a string.
	 * @return The list of QuerySolutions.
	 */
	public List<QuerySolution> executeQuery( final String queryStr )
	{
		return executeQuery(QueryFactory.create(queryStr));
	}

	public ModelReader getModelReader()
	{
		if (isService())
		{
			throw new IllegalStateException(
					"getModelReader() may not be called on a service catalog");
		}
		return new ModelReader() {
			@Override
			public void read( final Model model )
			{
				localModel = model;
			}
		};
	}

	public Node getServiceNode()
	{
		return isService() ? Node.createURI(sparqlEndpoint.toExternalForm())
				: null;
	}

	/**
	 * Create a sparql schema that has an empty namespace.
	 * 
	 * @return The Schema.
	 */
	public SparqlSchema getViewSchema()
	{
		return new SparqlSchema(this, SparqlView.NAME_SPACE, "");
	}

	public boolean isService()
	{
		return sparqlEndpoint != null;
	}

}
