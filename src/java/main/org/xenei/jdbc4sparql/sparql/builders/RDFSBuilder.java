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

import com.hp.hpl.jena.rdf.model.InfModel;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumnDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfKey;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;

/**
 *
 */
public class RDFSBuilder implements SchemaBuilder
{
	private final List<String> skip;
	private final InfModel rdfsOntology;

	public RDFSBuilder( final Model rdfsOntology )
	{
		this.rdfsOntology = ModelFactory.createRDFSModel(rdfsOntology);
		skip = new ArrayList<String>();
		skip.add(RDFS.getURI());
		skip.add(RDF.getURI());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder#getTableDefs(org.
	 * xenei.jdbc4sparql.sparql.SparqlCatalog)
	 */
	@Override
	public Set<TableDef> getTableDefs( final RdfCatalog catalog )
	{
		final Map<String, RdfTableDef> tables = new HashMap<String, RdfTableDef>();
		for (final Statement stmt : rdfsOntology.listStatements(null,
				RDFS.domain, (RDFNode) null).toList())
		{
			final RDFNode r = stmt.getObject();
			final String idTable = r.asNode().getLocalName() + "_ID";
			final String dataTbl = r.asNode().getLocalName() + "_data";
			if (!skip.contains(r.asNode().getNameSpace()))
			{
				Key pk = null;
				RdfTableDef idDef = tables.get(idTable);
				if (idDef == null)
				{
					final RdfTableDef.Builder builder = new RdfTableDef.Builder();
					idDef = new RdfTableDef(
							r.asNode().getNameSpace(),
							idTable,
							"%1$s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> %2$s",
							null);
					tables.put(idTable, idDef);
					final RdfColumnDef.Builder bldr = new RdfColumnDef.Builder();
					bldr.addQuerySegment("BIND( %2$s, %1$s )")
							// .setNamespace(r.asNode().getNameSpace())
							// .setLocalName(idTable)
							.setType(Types.VARCHAR).setSigned(false)
							.setNullable(DatabaseMetaData.columnNoNulls);
					idDef.add(bldr.build());
					pk = new RdfKey.Builder()
							.setUnique(true)
							.addSegment(
									new Builder().setIdx(0).setAscending(true))
							.build(model);
					idDef.setPrimaryKey(pk);
				}
				else
				{
					pk = idDef.getPrimaryKey();
				}

				RdfTableDef idData = tables.get(dataTbl);
				if (idData == null)
				{
					idData = new RdfTableDef(
							r.asNode().getNameSpace(),
							dataTbl,
							"%1$s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> %2$s",
							null);
					tables.put(dataTbl, idData);
					idData.add(idDef.getColumnDef(0));
				}
				final SparqlColumnDef.Builder bldr = new SparqlColumnDef.Builder();
				bldr.addQuerySegment("%1$s %3$s %2$s")
						.setNamespace(stmt.getSubject().getNameSpace())
						.setLocalName(stmt.getSubject().getLocalName())
						.setType(Types.VARCHAR).setSigned(false)
						.setNullable(DatabaseMetaData.columnNullable);

				idData.add(bldr.build());
			}
		}
		final HashSet<TableDef> retval = new HashSet<TableDef>();
		retval.addAll(tables.values());
		return retval;
	}

	public void setSchema( final String schema )
	{
	}

}
