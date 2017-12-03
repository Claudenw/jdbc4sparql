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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumnDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfKey;
import org.xenei.jdbc4sparql.impl.rdf.RdfKeySegment;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jena.entities.EntityManager;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.iterator.Filter;
import org.apache.jena.util.iterator.Map1;
import org.apache.jena.util.iterator.WrappedIterator;

/**
 * A simple builder that builds tables for all subjects of [?x a rdfs:Class]
 * triples. Columns for the tables are created from all predicates of all
 * instances of the class.
 */
public class SimpleNormalizingBuilder extends SimpleBuilder {
	public static final String BUILDER_NAME = "Smpl_Norm_Builder";
	public static final String DESCRIPTION = "A simple normalizing schema builder that builds tables based on RDFS Class names";

	// Params: namespace.
	protected static final String TABLE_QUERY = " prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
			+ " SELECT ?tName WHERE { ?tName a rdfs:Class . }";

	// Params: class resource, namespace
	protected static final String COLUMN_QUERY = "SELECT DISTINCT ?cName "
			+ " WHERE { ?instance a <%s> ; ?cName [] ; }";

	private static final String TABLE_SEGMENT = "%1$s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <%2$s> .";
	protected static final String COLUMN_SEGMENT = "%1$s <%3$s> %2$s .";
	protected static final String ID_COLUMN_SEGMENT = "BIND( %1$s as %2$s)";

	public RdfColumnDef idColumnDef;

	public SimpleNormalizingBuilder() {
	}

	protected Map<String, String> addColumnDefs(final Set<RdfTable> tableSet,
			final RdfCatalog catalog, final RdfSchema schema,
			final RdfTableDef.Builder tableDefBuilder, final Resource tName,
			final String tableQuerySegment) {
		final Map<String, String> colNames = new LinkedHashMap<String, String>();
		final List<QuerySolution> solns = catalog.executeQuery(String.format(
				SimpleNormalizingBuilder.COLUMN_QUERY, tName));

		colNames.put("id", SimpleNormalizingBuilder.ID_COLUMN_SEGMENT);

		tableDefBuilder.addColumnDef(idColumnDef);

		for (final QuerySolution soln : solns) {
			final RdfColumnDef.Builder builder = new RdfColumnDef.Builder();
			final Resource cName = soln.getResource("cName");
			final String columnQuerySegment = String.format(
					SimpleNormalizingBuilder.COLUMN_SEGMENT, "%1$s", "%2$s",
					cName.getURI());

			if (multipleCardinality(catalog, tableQuerySegment,
					columnQuerySegment)) {
				tableSet.add(makeSubTable(catalog, schema, tableQuerySegment,
						columnQuerySegment, tName, cName));
			}
			else {
				// might be a duplicate name
				if (colNames.containsKey(cName.getLocalName())) {
					int i = 2;
					while (colNames.containsKey(cName.getLocalName() + i)) {
						i++;
					}
					colNames.put(cName.getLocalName() + i, columnQuerySegment);
				}
				else {
					colNames.put(cName.getLocalName(), columnQuerySegment);
				}
				final int scale = calculateSize(catalog, tableQuerySegment,
						columnQuerySegment);
				builder.setType(Types.VARCHAR)
				.setNullable(DatabaseMetaData.columnNullable)
				.setScale(scale).setReadOnly(true);
				tableDefBuilder.addColumnDef(builder.build(catalog.getEntityManager()));
			}
		}
		return colNames;
	}

	private void buildTable(final Set<RdfTable> tableSet,
			final RdfCatalog catalog, final RdfSchema schema,
			final Resource tName) {
		final EntityManager entityManager = schema.getEntityManager();
		final RdfTableDef.Builder builder = new RdfTableDef.Builder();
		final String tableQuerySegment = String.format(
				SimpleNormalizingBuilder.TABLE_SEGMENT, "%1$s", tName.getURI());
		final Map<String, String> colNames = addColumnDefs(tableSet, catalog,
				schema, builder, tName, tableQuerySegment);

		if (colNames.size() > 1) {
			// add the key segments
			final RdfKeySegment.Builder keySegBuilder = new RdfKeySegment.Builder();
			keySegBuilder.setAscending(true);
			keySegBuilder.setIdx(0);

			final RdfKey.Builder keyBldr = new RdfKey.Builder();
			keyBldr.setUnique(true);
			keyBldr.addSegment(keySegBuilder.build(entityManager));

			builder.setPrimaryKey(keyBldr.build(entityManager));

			// build the def
			final RdfTableDef tableDef = builder.build(entityManager);

			// build the table
			final RdfTable.Builder tblBuilder = new RdfTable.Builder()
			.setTableDef(tableDef)
			.addQuerySegment(tableQuerySegment)
			.setName(tName.getLocalName())
			.setSchema(schema)
			.setRemarks(
					"created by "
							+ SimpleNormalizingBuilder.BUILDER_NAME);

			if (colNames.keySet().size() != tableDef.getColumnCount()) {
				throw new IllegalArgumentException(String.format(
						"There must be %s column names, %s provided",
						tableDef.getColumnCount(), colNames.keySet().size()));
			}

			// set the columns
			final Iterator<String> iter = colNames.keySet().iterator();
			int i = 0;
			while (iter.hasNext()) {

				final String cName = iter.next();
				tblBuilder
				.setColumn(i, cName)
				.getColumn(i)
				.addQuerySegment(colNames.get(cName))
				.setRemarks(
						"created by "
								+ SimpleNormalizingBuilder.BUILDER_NAME);
				i++;
			}

			tableSet.add(tblBuilder.build());
		}

	}

	@Override
	protected int calculateSize(final RdfCatalog catalog, final String tableQS,
			final String columnQS) {
		final String queryStr = String.format(
				"SELECT distinct ?col WHERE { %s %s }",
				String.format(tableQS, "?tbl"),
				String.format(columnQS, "?tbl", "?col"));
		final List<QuerySolution> results = catalog.executeQuery(queryStr);

		final Iterator<Integer> iter = WrappedIterator.create(
				results.iterator()).mapWith(new Function<QuerySolution, Integer>() {

					@Override
					public Integer apply(final QuerySolution o) {
						final RDFNode node = o.get("col");
						if (node == null) {
							return 0;
						}
						if (node.isLiteral()) {
							return TypeConverter.getJavaValue(node.asLiteral())
									.toString().length();
						}
						return node.toString().length();
					}
				});
		int retval = 0;
		while (iter.hasNext()) {
			final Integer i = iter.next();
			if (retval < i) {
				retval = i;
			}
		}
		return retval;

	}

	@Override
	public Set<RdfTable> getTables(final RdfSchema schema) {
		final RdfCatalog catalog = schema.getCatalog();
		final RdfColumnDef.Builder colBuilder = new RdfColumnDef.Builder();
		colBuilder.setType(Types.VARCHAR)
		.setNullable(DatabaseMetaData.columnNoNulls).setReadOnly(true);
		// FIXME does this need a scale?
		idColumnDef = colBuilder.build(schema.getEntityManager());
		final HashSet<RdfTable> retval = new HashSet<RdfTable>();
		final List<QuerySolution> solns = catalog
				.executeQuery(SimpleNormalizingBuilder.TABLE_QUERY);
		for (final QuerySolution soln : solns) {
			buildTable(retval, catalog, schema, soln.getResource("tName"));
		}
		return retval;
	}

	protected RdfTable makeSubTable(final RdfCatalog catalog,
			final RdfSchema schema, final String tableQuerySegment,
			final String columnQuerySegment, final Resource tName,
			final Resource cName) {
		final EntityManager entityManager = catalog.getEntityManager();

		final RdfTableDef.Builder tblDefBuilder = new RdfTableDef.Builder();

		tblDefBuilder.addColumnDef(idColumnDef);
		final RdfColumnDef.Builder builder = new RdfColumnDef.Builder();

		final int scale = calculateSize(catalog, tableQuerySegment,
				columnQuerySegment);
		builder.setType(Types.VARCHAR)
		.setNullable(DatabaseMetaData.columnNoNulls).setScale(scale)
		.setReadOnly(true);
		tblDefBuilder.addColumnDef(builder.build(entityManager));

		// add the key segments
		RdfKeySegment.Builder keySegBuilder = new RdfKeySegment.Builder();
		keySegBuilder.setAscending(true);
		keySegBuilder.setIdx(0);

		final RdfKey.Builder keyBldr = new RdfKey.Builder();
		keyBldr.setUnique(true);
		keyBldr.addSegment(keySegBuilder.build(entityManager));

		keySegBuilder = new RdfKeySegment.Builder();
		keySegBuilder.setAscending(true);
		keySegBuilder.setIdx(1);
		keyBldr.addSegment(keySegBuilder.build(entityManager));

		tblDefBuilder.setPrimaryKey(keyBldr.build(entityManager));

		// FIXME add foreign keys when avail

		final String tblName = String.format("%s%s", tName.getLocalName(),
				cName.getLocalName());

		final RdfTableDef tableDef = tblDefBuilder.build(entityManager);
		final RdfTable.Builder tblBuilder = new RdfTable.Builder()
		.setTableDef(tableDef)
		.addQuerySegment(tableQuerySegment)
		.setName(tblName)
		.setSchema(schema)
		.setRemarks(
				"created by " + SimpleNormalizingBuilder.BUILDER_NAME);

		tblBuilder
		.setColumn(0, "id")
		.getColumn(0)
		.addQuerySegment(SimpleNormalizingBuilder.ID_COLUMN_SEGMENT)
		.setRemarks(
				"created by " + SimpleNormalizingBuilder.BUILDER_NAME);

		tblBuilder
		.setColumn(1, cName.getLocalName())
		.getColumn(1)
		.addQuerySegment(columnQuerySegment)
		.setRemarks(
				"created by " + SimpleNormalizingBuilder.BUILDER_NAME);

		return tblBuilder.build();
	}

	protected boolean multipleCardinality(final RdfCatalog catalog,
			final String tableQS, final String columnQS) {
		final String queryStr = String.format(
				"SELECT (count(*) as ?count) WHERE { %s %s } GROUP BY ?tbl",
				String.format(tableQS, "?tbl"),
				String.format(columnQS, "?tbl", "?col"));

		return WrappedIterator
				.create(catalog.executeQuery(queryStr).iterator())
				.filterKeep(new Filter<QuerySolution>() {

					@Override
					public boolean accept(final QuerySolution o) {
						return o.get("count").asLiteral().getInt() > 1;
					}
				}).hasNext();

	}
}
