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

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumnDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * A simple example builder that looks for the phrase "nullable" in column name
 * to determine if they are nullable or not. if Nullable is not found
 * columnNoNulls is set. if the name contains int the colum type will be set to
 * "integer" otherwise it is string.
 */
public class SimpleNullableBuilder extends SimpleBuilder {
	public static final String BUILDER_NAME = "Simple_nullable_Builder";
	public static final String DESCRIPTION = "A simple schema builder extends Simple_Builder by addint nullable columns for columns that have 'nullable' in their names";

	public SimpleNullableBuilder() {
	}

	@Override
	protected Map<String, String> addColumnDefs(final RdfCatalog catalog,
			final RdfTableDef.Builder tableDefBuilder, final Resource tName,
			final String tableQuerySegment) {
		final Model model = catalog.getResource().getModel();
		final Map<String, String> colNames = new LinkedHashMap<String, String>();
		final List<QuerySolution> solns = catalog.executeQuery(String.format(
				SimpleBuilder.COLUMN_QUERY, tName));

		for (final QuerySolution soln : solns) {
			final RdfColumnDef.Builder builder = new RdfColumnDef.Builder();
			final Resource cName = soln.getResource("cName");
			int type = Types.VARCHAR;
			if (cName.getLocalName().contains("Int")) {
				type = Types.INTEGER;
			}
			if (cName.getLocalName().contains("Double")) {
				type = Types.DOUBLE;
			}
			if (cName.getLocalName().toLowerCase().contains("nullable")) {
				builder.setNullable(DatabaseMetaData.columnNullable);
			}
			else {
				builder.setNullable(DatabaseMetaData.columnNoNulls);
			}
			final String columnQuerySegment = String.format(
					SimpleBuilder.COLUMN_SEGMENT, "%1$s", "%2$s",
					cName.getURI());
			colNames.put(cName.getLocalName(), columnQuerySegment);
			final int scale = calculateSize(catalog, tableQuerySegment,
					columnQuerySegment);
			builder.setType(type).setScale(scale).setReadOnly(true);
			tableDefBuilder.addColumnDef(builder.build(model));
		}
		return colNames;
	}

}
