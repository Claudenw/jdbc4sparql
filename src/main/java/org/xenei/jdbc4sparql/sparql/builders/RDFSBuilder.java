/*
 * Copyright 2013, XENEI.com
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumnDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfKey;
import org.xenei.jdbc4sparql.impl.rdf.RdfKeySegment;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef.Builder;
import org.xenei.jdbc4sparql.utils.SQLNameUtil;

import com.hp.hpl.jena.rdf.model.InfModel;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 *
 */
public class RDFSBuilder implements SchemaBuilder {
	private final List<String> skip;

	public RDFSBuilder() {
		skip = new ArrayList<String>();
		skip.add(RDFS.getURI());
		skip.add(RDF.getURI());
	}

	private void addDataTableColumn(final Model model,
			final Map<String, RdfTableDef.Builder> tables,
			final Map<String, List<Resource>> columnName, final Statement stmt,
			final RdfTableDef.Builder idDef) {
		final String dataTbl = stmt.getObject().asNode().getURI() + "_data";
		RdfTableDef.Builder idData = tables.get(dataTbl);
		if (idData == null) {
			idData = new RdfTableDef.Builder();
			// r.asNode().getNameSpace(),
			// dataTbl,
			// "%1$s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> %2$s",
			// null);
			tables.put(dataTbl, idData);
			idData.addColumnDef(idDef.getColumnDef(0));
		}
		final RdfColumnDef.Builder bldr = new RdfColumnDef.Builder();
		List<Resource> colNames = columnName.get(dataTbl);
		if (colNames == null) {
			colNames = new ArrayList<Resource>();
			columnName.put(dataTbl, colNames);
		}
		colNames.add(stmt.getSubject());
		bldr.setType(Types.VARCHAR).setSigned(false)
		.setNullable(DatabaseMetaData.columnNullable);
		idData.addColumnDef(bldr.build(model));
	}

	private String fixupLiteral(final Literal l) {
		return SQLNameUtil.clean(l.getValue().toString());
	}

	private String getComment(final Resource cr) {
		RDFNode comment = null;
		Literal value = null;
		final StmtIterator iter = cr.listProperties(RDFS.comment);
		while (iter.hasNext()) {

			final Statement s = iter.next();
			comment = s.getObject();
			if (comment.isLiteral()) {
				if (value == null) {
					value = comment.asLiteral();
				}
				if (comment.asLiteral().getLanguage().equals("en")) {
					return comment.asLiteral().getValue().toString();
				}
			}
		}
		if (value != null) {
			return value.getValue().toString();
		}
		else {
			return "created by RDFSBuilder";
		}
	}

	private String getName(final Resource cr) {
		Literal value = null;
		final StmtIterator iter = cr.listProperties(RDFS.label);
		while (iter.hasNext()) {
			final Statement s = iter.next();
			if (s.getObject().isLiteral()) {
				if (value == null) {
					value = s.getObject().asLiteral();
				}
				if (s.getObject().asLiteral().getLanguage().equals("en")) {
					return fixupLiteral(s.getObject().asLiteral());
				}
			}
		}
		if (value != null) {
			return fixupLiteral(value);
		}
		else {
			final String prfx = cr.getModel().getNsURIPrefix(cr.getNameSpace());
			if (prfx != null) {
				return String.format("%s_%s", prfx, cr.getLocalName());
			}
			else {
				return cr.getLocalName();
			}
		}
	}

	private RdfTableDef.Builder getOrCreateIDTable(final Model model,
			final Map<String, RdfTableDef.Builder> tables, final RDFNode r) {
		final String idTable = r.asNode().getURI() + "_ID";

		// get or create the ID table.
		RdfTableDef.Builder idDef = tables.get(idTable);
		if (idDef == null) {
			idDef = new RdfTableDef.Builder();
			/*
			 * r.asNode().getNameSpace(), idTable,
			 * "%1$s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> %2$s" ,
			 * null);
			 */
			tables.put(idTable, idDef);
			final RdfColumnDef.Builder bldr = new RdfColumnDef.Builder();
			bldr // .addQuerySegment("BIND( %2$s, %1$s )")
			// .setNamespace(r.asNode().getNameSpace())
			// .setLocalName(idTable)
			.setType(Types.VARCHAR).setSigned(false)
			.setNullable(DatabaseMetaData.columnNoNulls);
			idDef.addColumnDef(bldr.build(model));
			idDef.setPrimaryKey(new RdfKey.Builder()
			.setUnique(true)
			.addSegment(
					new RdfKeySegment.Builder().setIdx(0)
					.setAscending(true).build(model))
					.build(model));
		}

		return idDef;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder#getTableDefs(org.
	 * xenei.jdbc4sparql.sparql.SparqlCatalog)
	 */
	@Override
	public Set<RdfTable> getTables(final RdfSchema schema) {
		final RdfCatalog catalog = schema.getCatalog();
		final InfModel rdfsOntology = ModelFactory.createRDFSModel(catalog
				.getLocalModel());

		// we have to build the table defs piece by piece
		final Model model = schema.getResource().getModel();
		final Map<String, RdfTableDef.Builder> tables = new HashMap<String, Builder>();
		final Map<String, List<Resource>> columnName = new HashMap<String, List<Resource>>();
		for (final Statement stmt : rdfsOntology.listStatements(null,
				RDFS.domain, (RDFNode) null).toList()) {
			final RDFNode r = stmt.getObject();
			if (r.isURIResource() && !skip.contains(r.asNode().getNameSpace())) {
				final RdfTableDef.Builder idDef = getOrCreateIDTable(model,
						tables, r);
				addDataTableColumn(model, tables, columnName, stmt, idDef);
			}
		}
		// all the definitions are built so build the tables.
		final HashSet<RdfTable> retval = new HashSet<RdfTable>();
		for (final String fqName : tables.keySet()) {
			model.createResource(fqName);
			final RdfTable.Builder builder = new RdfTable.Builder()
			.setTableDef(tables.get(fqName).build(model))
			.setSchema(schema).setColumn(0, "id");
			builder.getColumn(0).addQuerySegment("BIND( %1$s AS %2$s ) . ");
			Resource baseR = null
					// .setRemarks(getComment( tblRes ))
					;
			if (fqName.endsWith("_ID")) {
				baseR = catalog.getLocalModel().getResource(
						fqName.substring(0, fqName.length() - 3));
			}
			else { // _data
				baseR = catalog.getLocalModel().getResource(
						fqName.substring(0, fqName.length() - 5));

				/*
				 * r.asNode().getNameSpace(), idTable,
				 * "%1$s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> %2$s"
				 * , null);
				 */
				// column
				// .addQuerySegment("%1$s %3$s %2$s")
				// .setNamespace(stmt.getSubject().getNameSpace())
				// .setLocalName(stmt.getSubject().getLocalName())
				final List<Resource> colNames = columnName.get(fqName);
				for (int i = 0; i < colNames.size(); i++) {
					final Resource cr = colNames.get(i);
					builder.setColumn(i + 1, getName(cr));
					builder.getColumn(i + 1)
					.setRemarks(getComment(cr))
					.addQuerySegment(
							String.format("%s <%s> %s .", "%1$s",
									colNames.get(i), "%2$s"));
				}
			}
			builder.addQuerySegment(
					String.format(
							"%s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <%s> .",
							"%1$s", baseR.getURI())).setName(getName(baseR))
							.setRemarks(getComment(baseR));
			retval.add(builder.build(model));
		}
		return retval;
	}

	public void setSchema(final String schema) {
	}

}
