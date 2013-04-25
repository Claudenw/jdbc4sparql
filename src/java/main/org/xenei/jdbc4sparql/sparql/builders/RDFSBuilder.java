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

import org.xenei.jdbc4sparql.config.ModelReader;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlColumnDef;
import org.xenei.jdbc4sparql.sparql.SparqlTableDef;

/**
 *
 */
public class RDFSBuilder implements SchemaBuilder
{
	private List<String> skip;
	private InfModel rdfsOntology;
	
	public RDFSBuilder( Model rdfsOntology ) {
		this.rdfsOntology = ModelFactory.createRDFSModel(  rdfsOntology);
		skip = new ArrayList<String>();
		skip.add( RDFS.getURI() );
		skip.add( RDF.getURI());
	}
	
	public void skipURI( String uri )
	{
		skip.add( uri );
	}

	/* (non-Javadoc)
	 * @see org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder#getTableDefs(org.xenei.jdbc4sparql.sparql.SparqlCatalog)
	 */
	@Override
	public Set<TableDef> getTableDefs( SparqlCatalog catalog )
	{
		Map<String, SparqlTableDef> tables = new HashMap<String,SparqlTableDef>();
		for (Statement stmt : rdfsOntology.listStatements(null, RDFS.domain, (RDFNode)null).toList())
		{
			RDFNode r = stmt.getObject();
			if (!skip.contains(r.asNode().getNameSpace()))
			{
				SparqlTableDef def = tables.get( r.toString() );
				if (def == null)
				{
					def = new SparqlTableDef( r.asNode().getNameSpace(), r.asNode().getLocalName(), "%1$s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> %2$s");
					tables.put( r.toString(), def);
				}
				
				SparqlColumnDef.Builder bldr = new SparqlColumnDef.Builder();
				bldr.addQuerySegment("%1$s %3$s %2$s")
					.setNamespace(stmt.getSubject().getNameSpace())
						.setLocalName(stmt.getSubject().getLocalName())
						.setType( Types.VARCHAR )
						.setSigned( false )
						.setNullable( DatabaseMetaData.columnNullable );
				
				def.add( bldr.build() );
			}
		}
		HashSet<TableDef> retval = new HashSet<TableDef>();
		retval.addAll(tables.values());
		return retval;
	}

}
