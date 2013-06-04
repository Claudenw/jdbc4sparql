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
import com.hp.hpl.jena.rdf.model.Resource;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.List;

import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumnDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;

/**
 * A builder that looks for the phrase "nullable" in column localname to
 * determine
 * if they are nullable or not. if Nullable is not found columnNoNulls is set.
 * if the localname contains int the colum type will be set to "integer"
 * otherwise
 * it is string.
 */
public class SimpleNullableBuilder extends SimpleBuilder
{
	public static final String BUILDER_NAME = "Simple_nullable_Builder";
	public static final String DESCRIPTION = "A simple schema builder extends Simple_Builder by addint nullable columns for columns that have 'nullable' in their names";

	public SimpleNullableBuilder()
	{
	}

	@Override
	protected void addColumnDefs( final RdfCatalog catalog,
			final RdfTableDef tableDef, final Resource tName )
	{
		final List<QuerySolution> solns = catalog.executeQuery(String.format(
				SimpleBuilder.COLUMN_QUERY, tName));
		for (final QuerySolution soln : solns)
		{
			final Resource cName = soln.getResource("cName");
			int type = Types.VARCHAR;
			if (cName.getLocalName().contains("Int"))
			{
				type = Types.INTEGER;
			}

			final RdfColumnDef.Builder builder = new RdfColumnDef.Builder();
			builder.addQuerySegment(SimpleBuilder.COLUMN_SEGMENT)
					.setName(cName.getLocalName()).setType(type);

			if (cName.getLocalName().toLowerCase().contains("nullable"))
			{
				builder.setNullable(DatabaseMetaData.columnNullable);
			}
			else
			{
				builder.setNullable(DatabaseMetaData.columnNoNulls);
			}
			tableDef.add(builder.build());
		}
	}

}
