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
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.impl.AbstractTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.TableBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlTable extends RdfTable
{
	public static class Builder extends TableBuilder
	{
		public Builder()
		{
			super( SparqlTable.class, SparqlColumn.class );
			super.setType( "SPARQL TABLE" );
		}
		
		@Override
		public Builder setType( String ignore ) {
			// do nothing
			return this;
		}
	}
	private SparqlQueryBuilder builder;
	
	public Query getQuery() throws SQLException
	{
		if (builder == null)
		{
			builder = new SparqlQueryBuilder(getCatalog());
			builder.addTable(getSchema().getLocalName(), getLocalName());
			final Iterator<SparqlColumn> iter = getColumns();
			while (iter.hasNext())
			{
				final SparqlColumn col = iter.next();
				builder.addColumn(col);
				builder.addVar(col, col.getLocalName());
			}
		}
		return builder.build();
	}

	public List<Triple> getQuerySegments( final Node tableVar )
	{
		final List<Triple> retval = new ArrayList<Triple>();
		final String fqName = "<" + getFQName() + ">";
		for (final String segment : getTableDef().getQuerySegments())
		{
			if (!segment.trim().startsWith("#")) // skip comments
			{
				final List<String> parts = SparqlParser.Util
						.parseQuerySegment(String.format(segment, tableVar,
								fqName));
				if (parts.size() != 3)
				{
					throw new IllegalStateException(getFQName()
							+ " query segment " + segment
							+ " does not parse into 3 components");
				}
				retval.add(new Triple(
						SparqlParser.Util.parseNode(parts.get(0)),
						SparqlParser.Util.parseNode(parts.get(1)),
						SparqlParser.Util.parseNode(parts.get(2))));
			}
		}
		return retval;
	}

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		return new SparqlResultSet(this);
	}

	@Override
	public SparqlSchema getSchema()
	{
		return (SparqlSchema) super.getSchema();
	}

	public String getSolutionName( final int idx )
	{
		return builder.getSolutionName(idx);
	}

	@Override
	public SparqlTable getSuperTable()
	{
		if (superTable == null)
		{
			if (getSuperTableDef() == null)
			{
				return null;
			}
			superTable = (SparqlTable) getSchema().getTable(
					getSuperTableDef().getLocalName());
			if (superTable == null)
			{
				superTable = new SparqlTable(getSchema(),
						(SparqlTableDef) getSuperTableDef());
				getSchema().addTable(superTable);
			}
		}
		return superTable;
	}

	@Override
	public SparqlTableDef getTableDef()
	{
		return (SparqlTableDef) super.getTableDef();
	}

	@Override
	public String getType()
	{
		return "SPARQL TABLE";
	}
}
