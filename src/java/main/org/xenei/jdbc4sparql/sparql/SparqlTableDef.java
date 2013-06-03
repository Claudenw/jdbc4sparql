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

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;

import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.TableDefImpl;
import org.xenei.jdbc4sparql.impl.rdf.ColumnDefBuilder;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.rdf.ResourceBuilder;
import org.xenei.jdbc4sparql.impl.rdf.TableDefBuilder;
import org.xenei.jdbc4sparql.sparql.SparqlColumnDef.Builder;

public class SparqlTableDef extends RdfTableDef
{

	public static class Builder extends TableDefBuilder
	{
		private final List<String> querySegments = new ArrayList<String>();
		
		public Builder() {
			super( SparqlTableDef.class, SparqlColumnDef.class );
			
		}
		
		public Builder addQuerySegment( final String querySegment )
		{
			querySegments.add(querySegment);
			return this;
		}
		
		@Override
		public SparqlTableDef build(Model model)
		{
			SparqlTableDef def = (SparqlTableDef) super.build(model);
		
			
			RDFList lst = null;

			for (final String seg : querySegments)
			{
				final Literal l = model.createTypedLiteral( seg );
				if (lst == null)
				{
					lst = model.createList().with(l);
				}
				else
				{
					lst.add(l);
				}
			}
			ResourceBuilder builder = new ResourceBuilder( model );
			def.querySegmentLst = builder.getProperty(SparqlTableDef.class, "querySegments");
			def.getResource().addProperty(def.querySegmentLst, lst);
			querySegments.clear();
			return def;
		}
		
		@Override
		protected void checkBuildState()
		{
			super.checkBuildState();
			if (querySegments.size() == 0)
			{
				throw new IllegalStateException(
						"At least one query segment must be defined");
			}
		}
	}
	/**
	 * Query segments are string format strings where
	 * %1$s = table variable name
	 * %2$s = table FQ name.
	 * Multiple lines may be added. They will be added to the sparql query when
	 * the table is used.
	 * The string must have the form of a triple: S, P, O
	 * the components of the triple other than %1$s and %2$s must be fully
	 * qualified.
	 */
	private List<String> querySegments;
	private Property querySegmentLst;

	public List<String> getQuerySegments()
	{
		if (querySegments == null)
		{
			querySegments = new ArrayList<String>();
			Statement stmt = this.getResource().getRequiredProperty(querySegmentLst);
			RDFList lst = stmt.getObject().as( RDFList.class );
			for (RDFNode n : lst.asJavaList())
			{
				querySegments.add( n.asLiteral().getString() );
			}
		}
		return querySegments;
	}
}
