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

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.net.URL;
import java.util.List;

import org.xenei.jdbc4sparql.impl.CatalogImpl;

public class SparqlCatalog extends CatalogImpl
{
	private URL sparqlEndpoint;
	private Model localModel;

	public SparqlCatalog( final String namespace, final Model localModel,
			final String localName )
	{
		super(namespace, localName);
		this.localModel = localModel;
	}

	public SparqlCatalog( final URL sparqlEndpoint, final String localName )
	{
		super(sparqlEndpoint.toString(), localName);
		this.sparqlEndpoint = sparqlEndpoint;
	}

	public List<QuerySolution> executeQuery( final Query query )
	{
		QueryExecution qexec = null;
		if (localModel == null)
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

	public List<QuerySolution> executeQuery( final String queryStr )
	{
		return executeQuery(QueryFactory.create(queryStr));
	}

	public SparqlSchema getViewSchema()
	{
		return new SparqlSchema(this, SparqlView.NAME_SPACE, "");
	}

}
