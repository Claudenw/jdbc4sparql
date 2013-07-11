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
package org.xenei.jdbc4sparql.sparql.builders;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumnDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;

/**
 * A simple builder that builds tables for all subjects of [?x a rdfs:Class]
 * triples.
 * Columns for the tables are created from all predicates of all instances of
 * the class.
 */
public class SimpleBuilder implements SchemaBuilder
{
	public static final String BUILDER_NAME = "Simple_Builder";
	public static final String DESCRIPTION = "A simple schema builder that builds tables based on RDFS Class names";

	// Params: namespace.
	protected static final String TABLE_QUERY = " prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
			+ " SELECT ?tName WHERE { ?tName a rdfs:Class ; " + " }";

	// Params: class resource, namespace
	protected static final String COLUMN_QUERY = "SELECT DISTINCT ?cName "
			+ " WHERE { " + " ?instance a <%s> ; " + " ?cName [] ; }";

	private static final String TABLE_SEGMENT = "%1$s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <%2$s>";
	protected static final String COLUMN_SEGMENT = "%1$s <%3$s> %2$s";

	public SimpleBuilder()
	{
	}

	protected Map<String, String> addColumnDefs( final RdfCatalog catalog,
			final RdfTableDef.Builder tableDefBuilder, final Resource tName )
	{
		final Model model = catalog.getResource().getModel();
		final Map<String, String> colNames = new LinkedHashMap<String, String>();
		final List<QuerySolution> solns = catalog.executeQuery(String.format(
				SimpleBuilder.COLUMN_QUERY, tName));

		for (final QuerySolution soln : solns)
		{
			final RdfColumnDef.Builder builder = new RdfColumnDef.Builder();
			final Resource cName = soln.getResource("cName");
			final String s = String.format(SimpleBuilder.COLUMN_SEGMENT,
					"%1$s", "%2$s", cName.getURI());
			colNames.put(cName.getLocalName(), s);
			builder.setType(Types.VARCHAR).setNullable(
					DatabaseMetaData.columnNullable);
			tableDefBuilder.addColumnDef(builder.build(model));
		}
		return colNames;
	}

	@Override
	public Set<RdfTable> getTables( final RdfCatalog catalog )
	{
		final Model model = catalog.getResource().getModel();
		final HashSet<RdfTable> retval = new HashSet<RdfTable>();
		final List<QuerySolution> solns = catalog
				.executeQuery(SimpleBuilder.TABLE_QUERY);
		final RdfSchema schema = catalog.getSchema(Catalog.DEFAULT_SCHEMA);
		for (final QuerySolution soln : solns)
		{
			final Resource tName = soln.getResource("tName");
			final RdfTableDef.Builder builder = new RdfTableDef.Builder();
			final String s = String.format(SimpleBuilder.TABLE_SEGMENT, "%1$s",
					tName.getURI());
			final Map<String, String> colNames = addColumnDefs(catalog,
					builder, tName);
			final RdfTableDef tableDef = builder.build(model);
			final RdfTable.Builder tblBuilder = new RdfTable.Builder()
					.setTableDef(tableDef).addQuerySegment(s)
					.setName(tName.getLocalName()).setSchema(schema);

			if (colNames.keySet().size() != tableDef.getColumnCount())
			{
				throw new IllegalArgumentException(String.format(
						"There must be %s column names, %s provided",
						tableDef.getColumnCount(), colNames.keySet().size()));
			}
			final Iterator<String> iter = colNames.keySet().iterator();
			int i = 0;
			while (iter.hasNext())
			{

				final String cName = iter.next();
				tblBuilder.setColumn(i, cName).getColumn(i)
						.addQuerySegment(colNames.get(cName));
				i++;
			}
			retval.add(tblBuilder.build(model));
		}
		return retval;
	}

}
